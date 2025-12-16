## Quick Start
1) Create venv + install deps
2) Configure `.env`
3) Run Dagster: `dagster dev`

## Structure
- data/raw: original datasets (read-only)
- data/processed: cleaned/intermediate outputs
- database/sql: DDL + queries
- src: ingestion / preprocessing / storage / analysis utils
- orchestration: Dagster project
- results: figures + outputs
