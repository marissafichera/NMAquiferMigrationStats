#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build 'AMP_review' sheet from transfer metrics, sorted by NMAquifer_Table.Field,
and clean/normalize Error messages.

Input blocks:
  PointID|Table|Field|Error
  (some rows may have an extra error column; it will be appended to Error)

Output columns:
  - NMAquifer_Table.Field  (Table.Field; if Field blank -> just Table)
  - PointID
  - Error  (includes appended extra error text if present, cleaned)
"""

from pathlib import Path
import sys
import re

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ======= CONFIG — EDIT THESE =======
SERVICE_ACCOUNT_FILE = "service_account.json"
SPREADSHEET_ID = "1NtkaSWh8COQpMXd9AZ-fXMsRok9l-wwC1sz0lgVCTeo"
SHEET_NAME = "AMP_review"
TRANSFER_METRICS_PATH = r"transfer_metrics_metrics_2025-11-13T13_07_31.csv"
# ===================================

HEADER_CANON = "pointid|table|field|error"

def get_sheets_service(sa_path: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return build("sheets", "v4", credentials=creds)

def ensure_tab(service, spreadsheet_id: str, tab_name: str) -> int:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    by_title = {s["properties"]["title"]: s["properties"] for s in meta.get("sheets", [])}
    if tab_name in by_title:
        return by_title[tab_name]["sheetId"]
    req = {"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
    resp = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=req).execute()
    return resp["replies"][0]["addSheet"]["properties"]["sheetId"]

def clear_range(service, spreadsheet_id: str, tab_name: str, a1_range: str = "A:ZZ"):
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_name}'!{a1_range}"
    ).execute()

def write_values(service, spreadsheet_id: str, tab_name: str, start_cell: str, values):
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_name}'!{start_cell}",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


def canon(s: str) -> str:
    return (s or "").strip().lower()

# ---------- Error text normalization ----------
_row_id_re = re.compile(r"\brow\.id\s*=\s*\d+,\s*", re.IGNORECASE)

_sensor_type_re = re.compile(
    r"key\s+error\s+adding\s+sensor_type\s*:\s*(?P<stype>[^,|]+?)\s*error\s*:\s*'?(?P=stype)'?",
    re.IGNORECASE,
)

_org_missing_re = re.compile(
    r'key\s*\(organization\)\s*=\s*\((?P<org>[^)]+)\)\s*is\s*not\s*present\s*in\s*table\s*"?"?lexicon_term"?"?\.',
    re.IGNORECASE,
)

_value_error_prefix_re = re.compile(
    r"^\s*value\s*error\s*[,:\-]\s*", re.IGNORECASE
)

def clean_error(msg: str) -> str:
    """Normalize error messages per requested rules."""
    if not msg:
        return msg

    out = msg

    # 1) remove "row.ID=123, "
    out = _row_id_re.sub("", out)

    # 2) sensor_type phrasing → "Invalid sensor_type: <value>"
    m = _sensor_type_re.search(out)
    if m:
        stype = m.group("stype").strip()
        out = _sensor_type_re.sub(f"Invalid sensor_type: {stype}", out)

    # 3) organization missing → "Invalid organization: <org>"
    m = _org_missing_re.search(out)
    if m:
        org = m.group("org").strip()
        out = _org_missing_re.sub(f"Invalid organization: {org}", out)

    # 4) strip leading "Value error, " / "Value Error:" / "Value error -"
    out = _value_error_prefix_re.sub("", out)

    # --- NEW: remove quotes around/inside messages like
    # "Invalid organization: Santa Fe County; Santa Fe Animal Shelter"
    # Also removes smart quotes.
    out = out.replace('"', '').replace('“', '').replace('”', '')

    # 5) tidy whitespace
    out = re.sub(r"\s{2,}", " ", out).strip()
    # remove trailing commas/pipes/spaces
    out = re.sub(r"[\s\|,]+$", "", out)

    return out

def parse_amp_rows(path: Path):
    """
    Returns list of [NMAquifer_Table.Field, PointID, Error] across all header-delimited blocks.
    Appends any extra error columns to Error and cleans the final message.
    """
    if not path.exists():
        sys.exit(f"File not found: {path}")

    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        lines = [ln.rstrip("\n") for ln in f]

    rows_out = []
    n = len(lines)
    i = 0

    while i < n:
        line = lines[i].strip()
        # Find the next header
        if line and canon(line) == HEADER_CANON:
            i += 1
            while i < n:
                raw = lines[i]
                s = raw.strip()
                if not s:
                    break  # end of this block
                if canon(s) == HEADER_CANON:
                    break  # next block starts

                parts = [p.strip() for p in raw.split("|")]
                if len(parts) >= 4:
                    point_id = parts[0]
                    table    = parts[1]
                    field    = parts[2]
                    error    = parts[3]
                    extra    = " | ".join(parts[4:]).strip() if len(parts) > 4 else ""

                    combined_error = error if not extra else f"{error} | {extra}"
                    combined_error = clean_error(combined_error)

                    nm_tf = f"{table}.{field}" if field else table
                    rows_out.append([nm_tf, point_id, combined_error])

                i += 1
            continue
        i += 1

    return rows_out

def sort_rows_by_table_field(rows):
    """Sort case-insensitively by (table, field)."""
    def key_func(row):
        tf = row[0] or ""
        tf_lower = tf.lower()
        if "." in tf_lower:
            t, f = tf_lower.split(".", 1)
        else:
            t, f = tf_lower, ""  # table-only rows before specific fields
        return (t, f)
    return sorted(rows, key=key_func)

def main():
    data_rows = parse_amp_rows(Path(TRANSFER_METRICS_PATH))
    print(f"[info] Parsed {len(data_rows)} rows for AMP_review before sort.")
    data_rows = sort_rows_by_table_field(data_rows)
    print(f"[info] Sorted rows by NMAquifer_Table.Field.")

    headers = ["NMAquifer_Table.Field", "PointID", "Error"]
    values = [headers] + data_rows

    service = get_sheets_service(SERVICE_ACCOUNT_FILE)
    ensure_tab(service, SPREADSHEET_ID, SHEET_NAME)
    clear_range(service, SPREADSHEET_ID, SHEET_NAME, "A:C")
    write_values(service, SPREADSHEET_ID, SHEET_NAME, "A1", values)

    print(f"[done] Wrote AMP_review with {len(data_rows)} cleaned + sorted rows.")

if __name__ == "__main__":
    main()
