from __future__ import annotations
from pathlib import Path
import pandas as pd

def readcsv(file_path: Path) -> pd.DataFrame:
    """
    Reads a CSV file and returns a pandas DataFrame.
    """
    df = pd.read_csv(file_path)
    # Normalize column names (simple, safe)
    df.columns = [c.strip() for c in df.columns]
    return df
