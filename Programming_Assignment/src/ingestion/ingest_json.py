from __future__ import annotations
from calendar import c
from os import path, read
from pathlib import Path
import json
import pandas as pd

def read_json(file_path: Path) -> pd.DataFrame:
    """
    Reads a JSON file and returns a pandas DataFrame.
    """
    read_text = path.read_text(encoding='utf-8')
    data = json.loads(read_text)

    if isinstance(data, dict) and 'data' in data:
        data = data['data']

    if not isinstance(data, list):
        raise ValueError(f"Expected a list of objects, got {type(data)}")
    
    df = pd.json_normalize(data)
    # Convert any list/dict columns into JSON strings (so they can be stored as TEXT safely)
    for col in df.columns:
        has_nested = df[col].apply(lambda x: isinstance(x, (list, dict))).any()
        if has_nested:
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x
            )
    # wipe out any empty columns
    df.columns = [c.strip() for c in df.columns]

    return df