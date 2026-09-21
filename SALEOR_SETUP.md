# Saleor source application setup

The source is the official [Saleor Platform](https://github.com/saleor/saleor-platform) checkout under `infra/saleor-platform` at commit `ab6315bd59c58b4815175df4c679107ff9695be4`.

## Prerequisite

Start Docker Desktop with Linux containers and wait for the engine to become ready. Upstream recommends at least 5 GB of Docker memory. Confirm `docker info` succeeds before pulling images or starting services.

## First startup

From `D:\gaoxin_de\infra\saleor-platform`:

```powershell
docker compose config --quiet
docker compose run --rm api python3 manage.py migrate
docker compose run --rm api python3 manage.py populatedb --createsuperuser
docker compose up -d
docker compose ps
```

Run sample population once for a fresh database, not on every startup. Persistent volumes retain application data. Subsequent startup uses `docker compose up -d`; stopping uses `docker compose stop`.

The upstream stack includes development credentials and is for local synthetic-data use only. Its documented sample administrator is `admin@example.com` with password `admin`. Do not reuse those credentials elsewhere.

## Interfaces

- Dashboard: http://localhost:9000/
- GraphQL: http://localhost:8000/graphql/
- Test email: http://localhost:8025/
- Tracing UI: http://localhost:16686/

## Source acceptance check

1. Confirm migrations finish and API/dashboard are healthy.
2. Log in and inspect synthetic products, channels and orders.
3. Authenticate an API client without committing its token.
4. Verify the installed GraphQL schema for order creation, order lines, update timestamps and pagination.
5. Create a synthetic order through a supported API workflow and extract it into a JSONL file.
6. Make a supported order change and verify that a subsequent extraction exposes it.
7. Record exact working requests and resolved image digests for reproducibility.

The upstream Compose image tags are not all immutable digests. The repository commit records configuration provenance, not exact container contents.

## Current verification

Configuration validation does not prove runtime readiness. Startup, authentication and API operations remain pending until the Docker engine is available.
