# src/utils/config.py
from __future__ import annotations
import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# load .env if present
load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

@dataclass(frozen=True)
class PostgresConfig:
    host: str = os.getenv("PG_HOST", "localhost")
    port: int = int(os.getenv("PG_PORT", "5432"))
    db: str = os.getenv("PG_DB", "analytics_db")
    user: str = os.getenv("PG_USER", "postgres")
    password: str = os.getenv("PG_PASSWORD", "secret")

@dataclass(frozen=True)
class MongoConfig:
    uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db: str = os.getenv("MONGO_DB", "teamx_db")

def get_paths() -> dict[str, Path]:
    return {
        "root": PROJECT_ROOT,
        "data_raw": PROJECT_ROOT / "data" / "raw",
        "data_processed": PROJECT_ROOT / "data" / "processed",
        "data_sample": PROJECT_ROOT / "data" / "sample",
        "results_figures": PROJECT_ROOT / "results" / "figures",
        "results_outputs": PROJECT_ROOT / "results" / "outputs",
        "sql_dir": PROJECT_ROOT / "database" / "sql",
    }