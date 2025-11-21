#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Check Ocotillo_TableField entries in the FieldPairs_Checked sheet
against ocotillo_current_test.csv (Postgres export), and write an
ExistsInOcotillo column ("yes"/"no").

- DOES NOT write or modify the first two columns.
- Only reads NMAquifer_TableField (col1) and Ocotillo_TableField (col2).
- Writes/updates ExistsInOcotillo in its own column in FieldPairs_Checked.
"""

import sys
from pathlib import Path

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# =============== CONFIG =================

SERVICE_ACCOUNT_FILE = "service_account.json"

# Spreadsheet with FieldPairs_Checked
SPREADSHEET_ID = "1NtkaSWh8COQpMXd9AZ-fXMsRok9l-wwC1sz0lgVCTeo"

FIELDPAIRS_SHEET_NAME = "FieldPairs_Checked"

# Local CSV exported from Postgres query:
#   SELECT table_name || '.' || column_name AS table_field ...
OCOTILLO_CSV_PATH = "ocotillo_current.csv"

EXISTS_COL_NAME = "ExistsInOcotillo"

# =======================================


def normalize_key(x: str) -> str:
    if x is None:
        return ""
    return str(x).strip().lower()


def get_sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=scopes,
    )
    return build("sheets", "v4", credentials=creds)


def col_index_to_letter(idx: int) -> str:
    """0-based column index -> A1 column letter (0 -> A, 1 -> B, 2 -> C, ...)"""
    idx += 1  # 1-based
    letters = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def main():
    # 1) Load ocotillo_current_test.csv and build a set of <table>.<field> keys
    csv_path = Path(OCOTILLO_CSV_PATH)
    if not csv_path.exists():
        sys.exit(f"{OCOTILLO_CSV_PATH} not found. Export it from Postgres first.")

    df_oc = pd.read_csv(csv_path, dtype=str)
    if "table_field" not in df_oc.columns:
        sys.exit(f"{OCOTILLO_CSV_PATH} must have a 'table_field' column.")

    oc_set = set(df_oc["table_field"].dropna().map(normalize_key))
    print(f"Loaded {len(oc_set)} unique Ocotillo <table>.<field> entries.")

    # 2) Connect to Sheets and read FieldPairs_Checked (raw values)
    service = get_sheets_service()

    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{FIELDPAIRS_SHEET_NAME}'!A1:Z"
    ).execute()
    values = resp.get("values", [])
    if not values:
        sys.exit(f"Sheet '{FIELDPAIRS_SHEET_NAME}' is empty or not found.")

    headers = values[0]
    data_rows = values[1:]

    if len(headers) < 2:
        sys.exit(f"Expected at least two columns in '{FIELDPAIRS_SHEET_NAME}', got: {headers}")

    # First two columns:
    #   A: NMAquifer_TableField  (ignored)
    #   B: Ocotillo_TableField   (checked)
    oc_idx = 1

    # Find or create ExistsInOcotillo column index (header row may not have it yet)
    if EXISTS_COL_NAME in headers:
        exists_idx = headers.index(EXISTS_COL_NAME)
    else:
        exists_idx = len(headers)  # new column at end

    # Compute ExistsInOcotillo values from the second column
    def compute_exists(val: str) -> str:
        key = normalize_key(val)
        if not key:  # blank stays blank
            return ""
        if key in ("n/a", "na"):  # N/A variants
            return "N/A"
        return "yes" if key in oc_set else "no"

    exists_values = []
    for row in data_rows:
        oc_val_raw = row[oc_idx] if len(row) > oc_idx else ""
        exists_values.append(compute_exists(oc_val_raw))

    # Write ONLY the ExistsInOcotillo column back
    def col_index_to_letter(idx: int) -> str:
        idx += 1  # 1-based
        letters = ""
        while idx > 0:
            idx, rem = divmod(idx - 1, 26)
            letters = chr(65 + rem) + letters
        return letters

    col_letter = col_index_to_letter(exists_idx)
    num_rows = len(values)  # includes header
    col_values = [[EXISTS_COL_NAME]] + [[v] for v in exists_values]

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{FIELDPAIRS_SHEET_NAME}'!{col_letter}1:{col_letter}{num_rows}",
        valueInputOption="RAW",
        body={"values": col_values}
    ).execute()

    # --- Stats (excluding N/A) and summary to E1 ---
    # Use exists_values directly (no DataFrame)
    filtered = [v for v in exists_values if v in ("yes", "no", "")]
    total = len(filtered) or 1
    yes_c = sum(1 for v in filtered if v == "yes")
    no_c = sum(1 for v in filtered if v == "no")
    blank_c = sum(1 for v in filtered if v == "")

    def pct(n):
        return round(100.0 * n / total, 1)

    summary_values = [
        ["ExistsInOcotillo Summary (excluding N/A)", "", ""],
        ["Value", "Count", "Percent"],
        ["yes", int(yes_c), f"{pct(yes_c)}%"],
        ["no", int(no_c), f"{pct(no_c)}%"],
        ["blank", int(blank_c), f"{pct(blank_c)}%"],
    ]

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{FIELDPAIRS_SHEET_NAME}'!O1:Q5",
        valueInputOption="RAW",
        body={"values": summary_values}
    ).execute()

    print(
        f"✓ Updated {EXISTS_COL_NAME} in '{FIELDPAIRS_SHEET_NAME}' "
        f"(first two columns untouched)."
    )


if __name__ == "__main__":
    main()
