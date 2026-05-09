import hashlib
import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from paths import ensure_parent_dir, temp_excel_path


def read_url_rows(filename: str, include_pages: bool = True) -> Optional[List[Dict[str, Any]]]:
    try:
        df = pd.read_excel(filename, engine="openpyxl")
        if "url" not in df.columns:
            return None
        if include_pages:
            if "pages_to_scrape" not in df.columns:
                df["pages_to_scrape"] = None
            else:
                df["pages_to_scrape"] = df["pages_to_scrape"].apply(
                    lambda x: str(x) if pd.notna(x) and str(x).strip() != "nan" else None
                )
            return df[["url", "pages_to_scrape"]].dropna(subset=["url"]).to_dict("records")
        return df[["url"]].dropna(subset=["url"]).to_dict("records")
    except FileNotFoundError:
        raise
    except Exception:
        raise


def input_rows_fingerprint(filename: str, include_pages: bool = True) -> str:
    rows = read_url_rows(filename, include_pages=include_pages) or []
    normalized = []
    for row in rows:
        item = {"url": str(row.get("url", "")).strip()}
        if include_pages:
            pages = row.get("pages_to_scrape")
            item["pages_to_scrape"] = None if pages is None else str(pages).strip()
        normalized.append(item)
    payload = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def write_single_sheet_excel(rows: List[Dict[str, Any]], filename: str, columns: List[str], sheet_name: str) -> None:
    ensure_parent_dir(filename)
    temp_file = temp_excel_path(filename)
    df = pd.DataFrame(rows).reindex(columns=columns)
    with pd.ExcelWriter(temp_file, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    os.replace(temp_file, filename)


def write_multi_sheet_excel(filename: str, sheets: List[Dict[str, Any]]) -> None:
    ensure_parent_dir(filename)
    temp_file = temp_excel_path(filename)
    with pd.ExcelWriter(temp_file, engine="openpyxl") as writer:
        for sheet in sheets:
            df = pd.DataFrame(sheet["rows"]).reindex(columns=sheet["columns"])
            for col in sheet.get("text_columns", []):
                if col in df.columns:
                    df[col] = df[col].astype(str).replace({'nan': pd.NA, 'None': pd.NA, 'N/A': pd.NA})
            df.to_excel(writer, sheet_name=sheet["name"], index=False)
    os.replace(temp_file, filename)
