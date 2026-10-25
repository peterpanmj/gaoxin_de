"""Replayable ingestion and validated publication (assessment A/B/C/D).

File locks serialize local writers. A single atomic pointer commits the immutable
Parquet release and extraction watermarks. Checksums detect accidental changes;
they are not a security boundary against writers with filesystem access.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import time
from pathlib import Path
from typing import Any

from filelock import FileLock

from saleor_analytics.common import atomic_json, digest, identifier, now
from saleor_analytics.config import Settings
from saleor_analytics.records import CONTRACT_VERSION, RecordError, normalize_order
from saleor_analytics.spark_engine import build_and_validate, session, stage_accepted


def read_json(path: Path) -> dict:
    """Read a UTF-8 JSON object from an evidence or release metadata file."""
    return json.loads(path.read_text(encoding="utf-8"))


def fingerprint(value: Any) -> str:
    """Produce a stable SHA-256 fingerprint for JSON-compatible input."""
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def root_lock(settings: Settings) -> FileLock:
    """Return the process lock that serializes changes beneath the analytics root."""
    settings.root.mkdir(parents=True, exist_ok=True)
    return FileLock(str(settings.root / ".writer.lock"), timeout=60)


def current_release(settings: Settings) -> dict:
    """Return the published release pointer, or an empty object before first publication."""
    path = settings.root / "warehouse/current.json"
    return read_json(path) if path.exists() else {}


def dataset_digest(directory: Path) -> str:
    """Bind a release to every Parquet dataset file and its relative path."""
    if not directory.is_dir():
        raise FileNotFoundError(f"Release dataset not found: {directory}")
    files = sorted(path for path in directory.rglob("*") if path.is_file())
    if not files:
        raise RecordError("Release dataset is empty")
    value = hashlib.sha256()
    for path in files:
        value.update(path.relative_to(directory).as_posix().encode())
        value.update(digest(path).encode())
    return value.hexdigest()


def published_dataset(settings: Settings) -> tuple[Path, dict]:
    """Resolve one consistent serving release and reject altered contents."""
    pointer = current_release(settings)
    if not pointer:
        raise FileNotFoundError("No validated release; run build-warehouse first")
    if "dataset_sha256" not in pointer:
        raise RecordError("Legacy warehouse release; publish a new Spark release before serving")
    path = settings.root / "releases" / identifier(pointer["release_id"]) / "dataset"
    if dataset_digest(path) != pointer["dataset_sha256"]:
        raise RecordError("Published dataset checksum mismatch")
    return path, pointer


def export_artifacts(
    settings: Settings,
    destination: Path,
    release_id: str | None = None,
    *,
    all_snapshots: bool = False,
) -> dict:
    """Copy one release and selected input evidence into a review bundle (A/F)."""
    pointer = current_release(settings)
    selected_release_id = identifier(release_id) if release_id else pointer.get("release_id")
    if not selected_release_id:
        raise FileNotFoundError(
            "No validated release; supply --release-id or publish a release first"
        )
    release_directory = settings.root / "releases" / selected_release_id
    metadata_path = release_directory / "release.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Release metadata not found: {selected_release_id}")
    metadata = read_json(metadata_path)
    if metadata.get("release_id") != selected_release_id:
        raise RecordError("Release metadata does not match requested release ID")

    destination = destination.resolve()
    root = settings.root.resolve()
    if destination == root or root in destination.parents:
        raise ValueError("Export destination must be outside the analytics root")
    bundle = destination / selected_release_id
    if bundle.exists():
        raise FileExistsError(f"Export destination already exists: {bundle}")

    shutil.copytree(release_directory, bundle / "releases" / selected_release_id)
    inputs = metadata["inputs"]
    selected_inputs = (
        inputs if all_snapshots else [max(inputs, key=lambda item: item["extracted_at"])]
    )
    snapshot_ids = [item["snapshot_id"] for item in selected_inputs]
    for snapshot_id in snapshot_ids:
        source = settings.root / "bronze" / identifier(snapshot_id)
        if not source.exists():
            raise FileNotFoundError(f"Bronze evidence missing for snapshot: {snapshot_id}")
        shutil.copytree(source, bundle / "bronze" / snapshot_id)
        quarantine = settings.root / "quarantine" / snapshot_id
        if quarantine.exists():
            shutil.copytree(quarantine, bundle / "quarantine" / snapshot_id)
        request = settings.root / "extractions" / snapshot_id / "request.json"
        if request.exists():
            target = bundle / "extractions" / snapshot_id
            target.mkdir(parents=True, exist_ok=True)
            shutil.copy2(request, target / request.name)
    if pointer.get("release_id") == selected_release_id:
        target = bundle / "warehouse"
        target.mkdir(parents=True, exist_ok=True)
        shutil.copy2(settings.root / "warehouse/current.json", target / "current.json")
    atomic_json(
        bundle / "export.json",
        {
            "release_id": selected_release_id,
            "snapshot_ids": snapshot_ids,
            "all_snapshots": all_snapshots,
            "exported_at": now(),
        },
    )
    return {
        "export_directory": str(bundle),
        "release_id": selected_release_id,
        "snapshots": snapshot_ids,
    }


def verify_snapshot(directory: Path) -> dict:
    """Validate a Bronze manifest's contract version and retained file checksums."""
    manifest = read_json(directory / "manifest.json")
    if manifest.get("contract_version") != CONTRACT_VERSION:
        raise RecordError("Unsupported contract; reingest legacy raw JSONL using a new ID/root")
    for filename, key in [
        ("orders.raw.jsonl", "input_sha256"),
        ("orders.accepted.jsonl", "accepted_sha256"),
    ]:
        if digest(directory / filename) != manifest.get(key):
            raise RecordError(f"Snapshot checksum mismatch: {directory.name}/{filename}")
    return manifest


def ingest_jsonl(
    settings: Settings, input_path: Path, snapshot_id: str, *, extraction: dict | None = None
) -> dict[str, Any]:
    """Normalize JSONL, collapse identical versions, quarantine bad rows (B/C).

    Same-ID/same-bytes retries return the completed result. Changed input needs
    a new ID. Interrupted same-bytes attempts resume. Version conflicts always
    fail. Empty bounded API deltas pass; ordinary empty files fail.
    """
    started = time.monotonic()
    snapshot_id = identifier(snapshot_id)
    input_sha = digest(input_path)
    directory = settings.root / "bronze" / snapshot_id
    with root_lock(settings):
        manifest_path = directory / "manifest.json"
        if manifest_path.exists():
            manifest = verify_snapshot(directory)
            if manifest["input_sha256"] != input_sha or manifest.get("extraction") != extraction:
                raise RecordError("Snapshot ID already belongs to different input/window")
            if manifest["quality_gate"] != "passed":
                raise RecordError("Snapshot previously failed; correct input and use a new ID")
            return manifest
        directory.mkdir(parents=True, exist_ok=True)
        raw = directory / "orders.raw.jsonl"
        if raw.exists() and digest(raw) != input_sha:
            raise RecordError("Interrupted snapshot belongs to different input")
        if not raw.exists():
            shutil.copyfile(input_path, raw)
        accepted, rejected, seen = [], [], {}
        duplicates = conflicts = 0
        with raw.open(encoding="utf-8") as stream:
            for number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    order = normalize_order(json.loads(line)).as_dict()
                    key = (order["order_id"], order["updated_at"])
                    payload = fingerprint(order)
                    if key in seen:
                        if seen[key] == payload:
                            duplicates += 1
                            continue
                        conflicts += 1
                        raise RecordError(
                            "Conflicting payload for the same order and source version"
                        )
                    seen[key] = payload
                    accepted.append(order)
                except (json.JSONDecodeError, RecordError) as exc:
                    rejected.append(
                        {"line_number": number, "error": str(exc), "raw": line.rstrip()}
                    )
        accepted_path = directory / "orders.accepted.jsonl"
        rejected_path = settings.root / "quarantine" / snapshot_id / "orders.rejected.jsonl"
        for path, rows in [(accepted_path, accepted), (rejected_path, rejected)]:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8"
            )
        count = len(accepted) + len(rejected) + duplicates
        empty_delta = extraction is not None and extraction.get("mode") == "incremental"
        rate = len(rejected) / count if count else (0.0 if empty_delta else 1.0)
        passed = (count > 0 or empty_delta) and rate <= settings.reject_rate_limit and not conflicts
        manifest = {
            "snapshot_id": snapshot_id,
            "extracted_at": now(),
            "input_sha256": input_sha,
            "accepted_sha256": digest(accepted_path),
            "input_count": count,
            "accepted_count": len(accepted),
            "rejected_count": len(rejected),
            "duplicate_count": duplicates,
            "conflict_count": conflicts,
            "reject_rate": rate,
            "quality_gate": "passed" if passed else "failed",
            "extraction": extraction,
            "duration_seconds": round(time.monotonic() - started, 3),
            "contract_version": CONTRACT_VERSION,
        }
        atomic_json(manifest_path, manifest)
        if not passed:
            raise RecordError(
                f"Snapshot failed quality gate: reject rate {rate:.1%}, conflicts {conflicts}"
            )
        return manifest


def approved_inputs(settings: Settings) -> list[dict]:
    """Return checksum-verified, passing Bronze snapshots not marked as excluded."""
    inputs = []
    excluded_path = settings.root / "excluded-snapshots.json"
    excluded = read_json(excluded_path) if excluded_path.exists() else {}
    for path in sorted((settings.root / "bronze").glob("*/manifest.json")):
        if path.parent.name in excluded:
            continue
        if read_json(path).get("quality_gate") == "passed":
            manifest = verify_snapshot(path.parent)
            inputs.append(manifest | {"manifest_sha256": digest(path)})
    return inputs


def exclude_snapshot(settings: Settings, snapshot_id: str, reason: str) -> None:
    """Record an auditable exclusion of unpublished bad input; retain raw bytes."""
    identifier(snapshot_id)
    if not reason.strip():
        raise ValueError("A reason is required")
    with root_lock(settings):
        pointer = current_release(settings)
        if pointer:
            release = read_json(settings.root / "releases" / pointer["release_id"] / "release.json")
            if snapshot_id in {item["snapshot_id"] for item in release["inputs"]}:
                raise RecordError("Cannot exclude inputs of the current release")
        manifest = settings.root / "bronze" / snapshot_id / "manifest.json"
        read_json(manifest)
        path = settings.root / "excluded-snapshots.json"
        exclusions = read_json(path) if path.exists() else {}
        exclusions[snapshot_id] = {
            "reason": reason.strip(),
            "excluded_at": now(),
            "manifest_sha256": digest(manifest),
        }
        atomic_json(path, exclusions)


def build_candidate(settings: Settings, release_id: str) -> Path:
    """Stage approved version history and provenance; same-input retry resumes (A/B/D)."""
    release_id = identifier(release_id)
    directory = settings.root / "releases" / release_id
    dataset = directory / "dataset"
    with root_lock(settings), FileLock(str(settings.root / f".release-{release_id}.lock")):
        inputs = approved_inputs(settings)
        if not inputs:
            raise RecordError("No approved snapshots to build")
        current = current_release(settings)
        if current:
            prior = read_json(settings.root / "releases" / current["release_id"] / "release.json")
            identities = {item["snapshot_id"]: item["manifest_sha256"] for item in inputs}
            if any(
                identities.get(item["snapshot_id"]) != item["manifest_sha256"]
                for item in prior["inputs"]
            ):
                raise RecordError("Previously published source history is missing or altered")
        signature = fingerprint(inputs)
        metadata_path = directory / "release.json"
        if metadata_path.exists():
            old = read_json(metadata_path)
            if old["input_signature"] != signature:
                raise RecordError("Release ID belongs to different inputs; use a new ID")
            if (dataset / "staging/order_versions/_SUCCESS").exists():
                return dataset
        directory.mkdir(parents=True, exist_ok=True)
        if current_release(settings).get("release_id") == release_id:
            raise RecordError("Cannot rebuild a published release")
        spark = session()
        try:
            stage_accepted(spark, settings.root, inputs, dataset)
        finally:
            spark.stop()
        atomic_json(
            metadata_path,
            {
                "release_id": release_id,
                "created_at": now(),
                "base_release": current_release(settings).get("release_id"),
                "input_signature": signature,
                "inputs": inputs,
                "status": "staged",
                "contract_version": CONTRACT_VERSION,
            },
        )
    return dataset


def run_spark_build(dataset: Path) -> None:
    """Build Silver/Gold Parquet and bind Spark quality evidence to candidate bytes."""
    directory = dataset.resolve().parent
    root = directory.parent.parent
    release_id = identifier(directory.name)
    with FileLock(str(root / f".release-{release_id}.lock"), timeout=60):
        metadata = read_json(directory / "release.json")
        if metadata.get("status") == "validated":
            if metadata.get("dataset_sha256") != dataset_digest(dataset):
                raise RecordError("Validated candidate dataset checksum mismatch")
            return
        if current_release(Settings(root)).get("release_id") == release_id:
            raise RecordError("Cannot mutate a published release")
        model = Path(__file__).resolve().parent / "spark_engine.py"
        evidence = directory / "quality/results.json"
        metadata["status"] = "validating"
        metadata["model_checksums"] = {"spark_engine.py": digest(model)}
        atomic_json(directory / "release.json", metadata)
        started = time.monotonic()
        try:
            spark = session()
            try:
                tests = build_and_validate(spark, dataset, evidence)
            finally:
                spark.stop()
            metadata.update(
                status="validated",
                dataset_sha256=dataset_digest(dataset),
                results_sha256=digest(evidence),
                validated_at=now(),
                data_tests_passed=tests,
                duration_seconds=round(time.monotonic() - started, 3),
            )
        except Exception:
            metadata["status"] = "failed"
            atomic_json(directory / "release.json", metadata)
            raise
        atomic_json(directory / "release.json", metadata)


def publish_candidate(settings: Settings, dataset: Path) -> Path:
    """Validate provenance and atomically commit release plus watermarks (C/D).

    Foreign, altered and stale candidates fail. Publication never rewrites a
    dataset being read. Same-release retries are no-ops.
    """
    dataset = dataset.resolve()
    release_id = identifier(dataset.parent.name)
    expected = (settings.root / "releases" / release_id / "dataset").resolve()
    if dataset != expected:
        raise RecordError("Candidate must belong to this data root")
    with root_lock(settings), FileLock(str(settings.root / f".release-{release_id}.lock")):
        metadata = read_json(dataset.parent / "release.json")
        if metadata.get("status") != "validated" or metadata.get(
            "dataset_sha256"
        ) != dataset_digest(dataset):
            raise RecordError("Publication requires an unchanged Spark-validated candidate")
        if metadata.get("results_sha256") != digest(dataset.parent / "quality/results.json"):
            raise RecordError("Spark quality evidence checksum mismatch")
        for item in metadata["inputs"]:
            path = settings.root / "bronze" / item["snapshot_id"]
            verify_snapshot(path)
            if digest(path / "manifest.json") != item["manifest_sha256"]:
                raise RecordError("Release source evidence has changed")
        excluded_path = settings.root / "excluded-snapshots.json"
        excluded = read_json(excluded_path) if excluded_path.exists() else {}
        if any(item["snapshot_id"] in excluded for item in metadata["inputs"]):
            raise RecordError("Candidate includes an excluded snapshot")
        previous = current_release(settings)
        if previous.get("release_id") == release_id:
            return dataset
        if previous.get("release_id") != metadata["base_release"]:
            raise RecordError("Stale candidate; build a new release from the current base")
        watermarks = dict(previous.get("watermarks", {}))
        for item in metadata["inputs"]:
            extraction = item.get("extraction")
            if extraction and extraction["mode"] in {"full", "incremental"}:
                source, upper = extraction["source"], extraction["upper"]
                watermarks[source] = max(watermarks.get(source, ""), upper)
        pointer = {
            "release_id": release_id,
            "dataset_sha256": metadata["dataset_sha256"],
            "published_at": now(),
            "watermarks": watermarks,
            "data_tests_passed": metadata["data_tests_passed"],
            "snapshots": len(metadata["inputs"]),
            "accepted_count": sum(x["accepted_count"] for x in metadata["inputs"]),
            "rejected_count": sum(x["rejected_count"] for x in metadata["inputs"]),
            "duplicate_count": sum(x.get("duplicate_count", 0) for x in metadata["inputs"]),
        }
        atomic_json(settings.root / "warehouse/current.json", pointer)
    return dataset
