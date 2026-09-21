# Saleor Analytics Demo

A Principal Data Engineer interview project using Saleor as a synthetic commerce source, Python and Click for ingestion, dbt and DuckDB for analytical modeling, and Plotly Dash for reporting.

## Status

Design and source infrastructure configuration are available. Pipeline, dashboard and orchestration implementation are pending. The implementation budget is eight hours with AI assistance; documentation and demo preparation are separate.

## Project documentation

- [Design and assessment requirements](DEMO_DESIGN.md)
- [Saleor setup](SALEOR_SETUP.md)

## Clone

```bash
git clone --recurse-submodules https://github.com/peterpanmj/gaoxin_de.git
cd gaoxin_de
```

For an existing clone:

```bash
git submodule update --init --recursive
```

The official Saleor Platform repository is pinned as a submodule under `infra/saleor-platform`. Its upstream development configuration is intended only for local synthetic-data use. Runtime data and local credentials must not be committed.

All project documentation, code comments and user-facing text are in English.
