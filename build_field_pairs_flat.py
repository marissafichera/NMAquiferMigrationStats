#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Create a flat FieldPairs sheet with two columns:

  NMAquifer_TableField  = "<Old Table Name>.<Old Column Name>"
  Ocotillo_TableField   = "<Ocotillo Table Name>.<Ocotillo Field Name>"

Sources:
  - mapping_report_matched.csv
  - mapping_report_unmatched.csv

Output:
  - Google Sheets tab: FieldPairs
"""

from pathlib import Path
import sys
from collections import OrderedDict

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ========= CONFIG – EDIT THESE =========

SERVICE_ACCOUNT_FILE = "service_account.json"
SPREADSHEET_ID = "1NtkaSWh8COQpMXd9AZ-fXMsRok9l-wwC1sz0lgVCTeo"

MAPPED_CSV = "mapping_report_matched.csv"
UNMAPPED_CSV = "mapping_report_unmatched.csv"

OUTPUT_SHEET_NAME = "FieldPairs"

# ======================================


def get_sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=scopes,
    )
    return build("sheets", "v4", credentials=creds)


def get_spreadsheet_metadata(service):
    spreadsheet = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID
    ).execute()
    sheets = spreadsheet.get("sheets", [])
    by_title = {
        s["properties"]["title"]: s["properties"]
        for s in sheets
    }
    return by_title


def ensure_sheet(service, title: str) -> int:
    meta = get_spreadsheet_metadata(service)
    if title in meta:
        return meta[title]["sheetId"]

    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {"title": title}
                }
            }
        ]
    }
    resp = service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body=body
    ).execute()
    return resp["replies"][0]["addSheet"]["properties"]["sheetId"]


def main():
    # --- Load matched CSV ---
    mapped_path = Path(MAPPED_CSV)
    if not mapped_path.exists():
        sys.exit(f"Required file not found: {MAPPED_CSV} (run your main pipeline first)")

    df_m = pd.read_csv(mapped_path, dtype=str)

    required_cols_m = {
        "Old Table Name",
        "Old Column Name",
        "Ocotillo Table Name",
        "Ocotillo Field Name",
    }
    if not required_cols_m.issubset(df_m.columns):
        sys.exit(
            "mapping_report_matched.csv must contain columns: "
            "'Old Table Name', 'Old Column Name', 'Ocotillo Table Name', 'Ocotillo Field Name'."
        )

    # --- Load unmatched CSV (optional) ---
    df_u = pd.DataFrame(columns=["Old Table Name", "Old Column Name"])
    unmapped_path = Path(UNMAPPED_CSV)
    if unmapped_path.exists():
        tmp = pd.read_csv(unmapped_path, dtype=str)
        if {"Old Table Name", "Old Column Name"}.issubset(tmp.columns):
            df_u = tmp[["Old Table Name", "Old Column Name"]].copy()

    # --- Build mapping (Old Table/Field → Ocotillo TableField) ---
    mapping = {}  # (old_table, old_field) -> "OcotilloTable.OcotilloField"

    for _, row in df_m.iterrows():
        old_table = str(row["Old Table Name"]).strip()
        old_field = str(row["Old Column Name"]).strip()
        o_tab = "" if pd.isna(row["Ocotillo Table Name"]) else str(row["Ocotillo Table Name"]).strip()
        o_field = "" if pd.isna(row["Ocotillo Field Name"]) else str(row["Ocotillo Field Name"]).strip()

        if o_tab and o_field:
            formatted = f"{o_tab}.{o_field}"
        elif o_tab:
            formatted = f"{o_tab}."
        elif o_field:
            formatted = f".{o_field}"
        else:
            formatted = ""

        # If there are multiple matches, keep the first seen (simple assumption)
        key = (old_table, old_field)
        if key not in mapping:
            mapping[key] = formatted

    # --- Collect all unique (old_table, old_field) pairs from matched + unmatched ---
    pairs = OrderedDict()

    # from matched
    for _, row in df_m.iterrows():
        key = (str(row["Old Table Name"]).strip(), str(row["Old Column Name"]).strip())
        pairs.setdefault(key, None)

    # from unmatched
    for _, row in df_u.iterrows():
        key = (str(row["Old Table Name"]).strip(), str(row["Old Column Name"]).strip())
        pairs.setdefault(key, None)

    # --- Build flat DataFrame ---
    rows = []
    for (old_table, old_field) in sorted(pairs.keys()):
        old_tf = f"{old_table}.{old_field}"
        occ_tf = mapping.get((old_table, old_field), "")
        rows.append({
            "NMAquifer_TableField": old_tf,
            "Ocotillo_TableField": occ_tf,
        })

    df_flat = pd.DataFrame(rows, columns=["NMAquifer_TableField", "Ocotillo_TableField"])
    df_flat.to_csv("FieldPairs_flat.csv", index=False)  # optional local export

    # --- Write to Sheets ---
    service = get_sheets_service()
    ensure_sheet(service, OUTPUT_SHEET_NAME)

    # Clear sheet
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{OUTPUT_SHEET_NAME}'!A:Z"
    ).execute()

    values = [df_flat.columns.tolist()] + df_flat.fillna("").values.tolist()

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{OUTPUT_SHEET_NAME}'!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

    print(f"✓ Wrote FieldPairs sheet with {len(df_flat)} rows.")


if __name__ == "__main__":
    main()
