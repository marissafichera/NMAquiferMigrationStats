#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Build a new sheet with 2 columns per table:

For each Old Table Name:
  - Column 1: "NMAquifer_Field"  (old field names)
  - Column 2: "Ocotillo_TableField" (filled, when possible, as "<Ocotillo Table Name>, <Ocotillo Field Name>")

Layout example (for tables T1, T2, T3):

Row 1:  T1                ""         T2                ""         T3               ""
Row 2:  NMAquifer_Field   Ocotillo_TableField  NMAquifer_Field   Ocotillo_TableField  ...

Row 3+: NMAquifer_Field values for each table;
        Ocotillo_TableField uses mapping_report_matched.csv where available,
        blank where unmatched.

Data source:
  - mapping_report_matched.csv
  - mapping_report_unmatched.csv
(these come from nmaquifer_sheets_pipeline.py)

Output:
  - New sheet (tab) in the same Google Sheet, e.g. "FieldPairs"
"""

from collections import OrderedDict, defaultdict
from pathlib import Path
import sys

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ==========================
# CONFIG - EDIT THIS PART
# ==========================

# Path to your service account JSON key file
SERVICE_ACCOUNT_FILE = "service_account.json"

# Spreadsheet ID of your mapping Google Sheet
# (just the ID, not the full URL)
SPREADSHEET_ID = "1NtkaSWh8COQpMXd9AZ-fXMsRok9l-wwC1sz0lgVCTeo"

# Paths to the mapping report CSVs produced by the first script
MAPPED_CSV = "mapping_report_matched.csv"
UNMAPPED_CSV = "mapping_report_unmatched.csv"

# Name of the output sheet/tab to create/update
OUTPUT_SHEET_NAME = "FieldPairs"


# ==========================
# SHEETS HELPERS
# ==========================

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
    """
    Ensure a sheet with the given title exists.
    Returns its sheetId.
    """
    meta = get_spreadsheet_metadata(service)
    if title in meta:
        return meta[title]["sheetId"]

    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": title
                    }
                }
            }
        ]
    }
    resp = service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body=body
    ).execute()
    sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    return sheet_id


# ==========================
# MAIN
# ==========================

def main():
    # Load matched CSV (required)
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

    # Load unmatched CSV (optional)
    df_u = pd.DataFrame(columns=["Old Table Name", "Old Column Name"])
    unmapped_path = Path(UNMAPPED_CSV)
    if unmapped_path.exists():
        tmp = pd.read_csv(unmapped_path, dtype=str)
        if {"Old Table Name", "Old Column Name"}.issubset(tmp.columns):
            df_u = tmp[["Old Table Name", "Old Column Name"]].copy()

    # ------------------------------
    # Build mapping for Ocotillo_TableField
    # mapping[(table, old_field)] -> list of "<Ocotillo Table Name>, <Ocotillo Field Name>"
    # ------------------------------
    mapping = defaultdict(list)

    for _, row in df_m.iterrows():
        table = str(row["Old Table Name"]).strip()
        old_field = str(row["Old Column Name"]).strip()
        o_tab = "" if pd.isna(row["Ocotillo Table Name"]) else str(row["Ocotillo Table Name"]).strip()
        o_field = "" if pd.isna(row["Ocotillo Field Name"]) else str(row["Ocotillo Field Name"]).strip()

        # If both Ocotillo fields are empty, we could skip; but let's still show the table name if present.
        if not o_tab and not o_field:
            formatted = ""
        elif o_tab and o_field:
            formatted = f"{o_tab}, {o_field}"
        elif o_tab:
            formatted = f"{o_tab}, "
        else:
            formatted = f", {o_field}"

        # Only add non-empty formatted strings
        if formatted:
            mapping[(table, old_field)].append(formatted)

    # ------------------------------
    # Collect ordered list of tables (alphabetical)
    # ------------------------------
    tables = sorted(set(
        list(df_m["Old Table Name"].dropna().unique()) +
        list(df_u["Old Table Name"].dropna().unique())
    ))

    # ------------------------------
    # Build mapping: table -> ordered list of old fields
    # ------------------------------
    table_fields = OrderedDict()
    max_len = 0

    for table in tables:
        fields = []

        # Matched first (preserve original order)
        g_m = df_m[df_m["Old Table Name"] == table]
        for _, r in g_m.iterrows():
            fields.append(str(r["Old Column Name"]).strip())

        # Then unmatched (also preserve order)
        if not df_u.empty:
            g_u = df_u[df_u["Old Table Name"] == table]
            for _, r in g_u.iterrows():
                fields.append(str(r["Old Column Name"]).strip())

        # De-duplicate while preserving order
        seen = set()
        deduped = []
        for f in fields:
            if f not in seen:
                seen.add(f)
                deduped.append(f)

        table_fields[table] = deduped
        if len(deduped) > max_len:
            max_len = len(deduped)

    # ------------------------------
    # Build 2D values array for Sheets
    # Row 1: table names across (each block 2 columns)
    # Row 2: subheaders "NMAquifer_Field", "Ocotillo_TableField"
    # Row 3+: NMAquifer_Field values; Ocotillo_TableField filled from mapping where possible
    # ------------------------------
    header_row = []
    subheader_row = []

    for table in table_fields.keys():
        header_row.extend([table, ""])  # table over two columns
        subheader_row.extend(["NMAquifer_Field", "Ocotillo_TableField"])

    values = [header_row, subheader_row]

    # Data rows
    for i in range(max_len):
        row = []
        for table, fields in table_fields.items():
            # NMAquifer_Field
            if i < len(fields):
                field_name = fields[i]
                row.append(field_name)
                # Ocotillo_TableField: lookup mapping
                key = (table, field_name)
                ocotillo_values = mapping.get(key, [])
                if ocotillo_values:
                    # If multiple mappings exist, join them with " | "
                    row.append(" | ".join(ocotillo_values))
                else:
                    row.append("")
            else:
                # No field at this row for this table
                row.append("")
                row.append("")
        values.append(row)

    # ------------------------------
    # Write to Sheets
    # ------------------------------
    service = get_sheets_service()
    sheet_id = ensure_sheet(service, OUTPUT_SHEET_NAME)

    # Clear existing content
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{OUTPUT_SHEET_NAME}'!A:Z"
    ).execute()

    # Write new content starting at A1
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{OUTPUT_SHEET_NAME}'!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

    print(f"✓ Wrote {OUTPUT_SHEET_NAME} with {len(tables)} tables and {max_len} rows of fields.")


if __name__ == "__main__":
    main()
