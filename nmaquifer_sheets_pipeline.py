#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Google Sheets pipeline for NM Aquifer → Ocotillo migration tracking.

What it does, in one run:
1. Reads the old DB structure CSV:
   - NM_Aquifer_Testing_DB_tables_cols.csv
     columns: table_name, columns (comma-separated old column names)

2. Connects to a Google Sheet (your mapping workbook) via the Sheets API:
   - Expects mapping tabs named: NMAquifer_{old_table_name}
   - Each mapping tab should have columns:
       "NMAquifer Field Name"
       "Ocotillo Table Name"
       "Ocotillo Field Name"
       "Does field exist in Ocotillo?"
       "Note" (optional)

3. For each table in the CSV:
   - Finds the matching mapping tab (by stripping NMAquifer_ prefix).
   - For each old column, tries to match it to "NMAquifer Field Name"
     using a normalized key:
       lowercase + remove all non-alphanumeric characters
     (so "Well_ID", "well id", "WELL-ID" -> "wellid").

   - Builds:
       * Matched rows:
         Old Table Name
         Old Column Name
         NMAquifer Field Name
         Ocotillo Table Name
         Ocotillo Field Name
         Does field exist in Ocotillo?  (copied EXACTLY from sheet, so "N/A" stays "N/A")
         Note

       * Unmatched rows:
         Old Table Name
         Old Column Name
         Reason

4. Writes results back to the SAME Google Sheet into three tabs:
   - "Matched"
   - "Unmatched"
   - "Matrix"

   If they don't exist yet, they are created.

   "Matrix" is a visual table:
   - Columns: Old Table Names
   - Rows: Old Column Names for each table
   - Each cell text: "FieldName (Status)" where Status = Does field exist in Ocotillo?.

5. Adds conditional formatting rules on the "Matrix" tab:
   - If cell text CONTAINS "(yes)"  -> green
   - If cell text CONTAINS "(no)"   -> yellow
   - If cell text CONTAINS "(N/A)"  -> gray
   - (Anything else will be uncolored by default, but you can add a red rule later.)

Once the rules are there, they keep applying to new data each time you run this.
"""

import re
import json
import sys
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ==========================
# CONFIGURATION - EDIT THIS
# ==========================

# Path to your service account JSON key file
SERVICE_ACCOUNT_FILE = "service_account.json"

# Spreadsheet ID of your mapping Google Sheet
SPREADSHEET_ID = "1NtkaSWh8COQpMXd9AZ-fXMsRok9l-wwC1sz0lgVCTeo"

# Path to your CSV with old DB tables/columns
CSV_PATH = "NM_Aquifer_Testing_DB_tables_cols.csv"

# Prefix used for mapping sheets
SHEET_PREFIX = "NMAquifer_"

# Names for output sheets
MATCHED_SHEET_NAME = "Matched"
UNMATCHED_SHEET_NAME = "Unmatched"
MATRIX_SHEET_NAME = "Matrix"

# Apply conditional formatting rules on Matrix sheet?
APPLY_CONDITIONAL_FORMATTING = True

# Required columns in each mapping sheet
REQ_SHEET_COLS = [
    "NMAquifer Field Name",
    "Ocotillo Table Name",
    "Ocotillo Field Name",
    # "Does field exist in Ocotillo?" / "Does field exist in the model?" handled separately
    # "Note" is optional; we'll add if missing
]


# ==========================
# HELPER FUNCTIONS
# ==========================

def normalize_name(x: Any) -> str:
    """Strip and force to string; empty if NaN/None."""
    if pd.isna(x):
        return ""
    return str(x).strip()


def make_key(x: Any) -> str:
    """Create a permissive match key: lowercase, alphanumeric only."""
    if x is None:
        return ""
    try:
        import math
        if isinstance(x, float) and math.isnan(x):
            return ""
    except Exception:
        pass
    s = str(x).lower()
    return re.sub(r"[^0-9a-z]+", "", s)


def split_columns_cell(cell: Any):
    """Split the CSV 'columns' field into a list of column names."""
    if pd.isna(cell):
        return []
    raw = str(cell).strip().strip("[]")
    parts = [p.strip().strip('"').strip("'") for p in raw.split(",")]
    return [p for p in parts if p]


def strip_prefix_case_insensitive(name: str, prefix: str) -> str:
    if name.lower().startswith(prefix.lower()):
        return name[len(prefix):]
    return name


def get_sheets_service():
    """Authenticate and return a Sheets API service client."""
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=scopes,
    )
    service = build("sheets", "v4", credentials=creds)
    return service


def get_spreadsheet_metadata(service):
    """Fetch spreadsheet metadata (sheet titles & IDs)."""
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

    # Add new sheet
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


def read_mapping_sheets_to_dfs(service) -> Dict[str, pd.DataFrame]:
    """
    Read all mapping sheets whose titles start with SHEET_PREFIX
    and return dict: old_table_name -> DataFrame
    """
    meta = get_spreadsheet_metadata(service)
    mapping_sheets = [title for title in meta.keys()
                      if title.lower().startswith(SHEET_PREFIX.lower())]

    sheets_dict: Dict[str, pd.DataFrame] = {}

    for sheet_title in mapping_sheets:
        # old table name is sheet_title with prefix stripped
        old_table = strip_prefix_case_insensitive(sheet_title, SHEET_PREFIX)
        # Read A:Z range (adjust if you have more columns)
        range_a1 = f"'{sheet_title}'!A:Z"
        resp = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_a1
        ).execute()
        values = resp.get("values", [])
        if not values:
            df = pd.DataFrame()
        else:
            headers = values[0]
            rows = values[1:]
            df = pd.DataFrame(rows, columns=headers)

        # Ensure required columns exist
        for col in REQ_SHEET_COLS:
            if col not in df.columns:
                df[col] = pd.NA
        if "Note" not in df.columns:
            df["Note"] = pd.NA

        # Handle the status column name mismatch:
        # - Location sheet: "Does field exist in Ocotillo"
        # - Others:         "Does field exist in the model?"
        if "Does field exist in Ocotillo?" not in df.columns:
            if "Does field exist in the model?" in df.columns:
                df["Does field exist in Ocotillo?"] = df["Does field exist in the model?"]
            else:
                # Neither exists: create an empty one
                df["Does field exist in Ocotillo?"] = pd.NA

        # Build matching key from NMAquifer Field Name
        df["__key__"] = df["NMAquifer Field Name"].map(make_key)

        sheets_dict[normalize_name(old_table)] = df

    return sheets_dict


def df_to_values(df: pd.DataFrame):
    """Convert a DataFrame to a Sheets-compatible 2D list (including header)."""
    if df is None or df.empty:
        return []
    df = df.copy()
    df = df.fillna("")
    return [df.columns.tolist()] + df.values.tolist()


def write_df_to_sheet(service, df: pd.DataFrame, sheet_title: str):
    """Clear sheet and write DataFrame values to it."""
    ensure_sheet(service, sheet_title)
    values = df_to_values(df)
    # Clear existing content
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{sheet_title}'!A:Z"
    ).execute()
    if not values:
        return
    # Write new values
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{sheet_title}'!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


def add_conditional_formatting_for_matrix(service, sheet_id: int):
    """
    Add conditional formatting rules to the Matrix sheet:
      - "(yes)"  -> green
      - "(no)"   -> yellow
      - "(N/A)"  -> gray
      - anything else non-empty -> light red
    Applied to all rows from row 6 down (A6:...).
    """
    def rgb(r, g, b):
        return {
            "red": r / 255.0,
            "green": g / 255.0,
            "blue": b / 255.0,
        }

    requests = []

    # YES -> green
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{
                    "sheetId": sheet_id,
                    "startRowIndex": 6,  # row 7 down (skip legend + header)
                }],
                "booleanRule": {
                    "condition": {
                        "type": "TEXT_CONTAINS",
                        "values": [{"userEnteredValue": "(yes)"}]
                    },
                    "format": {
                        "backgroundColor": rgb(0, 200, 0)
                    }
                }
            },
            "index": 0
        }
    })

    # NO -> yellow
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{
                    "sheetId": sheet_id,
                    "startRowIndex": 6,
                }],
                "booleanRule": {
                    "condition": {
                        "type": "TEXT_CONTAINS",
                        "values": [{"userEnteredValue": "(no)"}]
                    },
                    "format": {
                        "backgroundColor": rgb(255, 255, 128)
                    }
                }
            },
            "index": 1
        }
    })

    # N/A -> gray
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{
                    "sheetId": sheet_id,
                    "startRowIndex": 6,
                }],
                "booleanRule": {
                    "condition": {
                        "type": "TEXT_CONTAINS",
                        "values": [{"userEnteredValue": "(N/A)"}]
                    },
                    "format": {
                        "backgroundColor": rgb(200, 200, 200)
                    }
                }
            },
            "index": 2
        }
    })

    # Catch-all: anything non-empty that is NOT yes/no/N/A -> light red
    # A6 is top-left of the range; formula is relative.
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{
                    "sheetId": sheet_id,
                    "startRowIndex": 6,
                }],
                "booleanRule": {
                    "condition": {
                        "type": "CUSTOM_FORMULA",
                        "values": [{
                            "userEnteredValue":
                                '=AND(LEN(A7)>0, NOT(REGEXMATCH(A7,"\\((yes|no|N/A)\\)")))'
                        }]
                    },
                    "format": {
                        "backgroundColor": rgb(255, 200, 200)  # light red
                    }
                }
            },
            "index": 3
        }
    })

    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": requests}
    ).execute()


def format_matrix_headers(service, sheet_id: int, headers, stats):
    """
    Color Matrix header row (row 5 / index 4):
      - Green if table has a mapping sheet (has_sheet=True)
      - Light red if it doesn't.
    """
    def rgb(r, g, b):
        return {
            "red": r / 255.0,
            "green": g / 255.0,
            "blue": b / 255.0,
        }

    requests = []
    for col_index, table_name in enumerate(headers):
        has_sheet = stats.get(table_name, {}).get("has_sheet", False)
        color = rgb(0, 200, 0) if has_sheet else rgb(255, 200, 200)  # green vs light red

        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 5,  # row 6 (A6)
                    "endRowIndex": 6,
                    "startColumnIndex": col_index,
                    "endColumnIndex": col_index + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": color,
                        "textFormat": {"bold": True}
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)"
            }
        })

    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": requests}
        ).execute()



# ==========================
# MAIN
# ==========================

def main():
    # Check CSV exists
    csv_path = Path(CSV_PATH)
    if not csv_path.exists():
        sys.exit(f"CSV not found: {csv_path}")

    # Read CSV
    try:
        df_csv = pd.read_csv(csv_path, dtype=str)
    except Exception as e:
        sys.exit(f"Failed to read CSV: {e}")

    if "table_name" not in df_csv.columns or "columns" not in df_csv.columns:
        sys.exit("CSV must contain 'table_name' and 'columns' columns.")

    # Connect to Sheets
    service = get_sheets_service()

    # Load mapping sheets into DataFrames
    sheets_dict = read_mapping_sheets_to_dfs(service)

    matched_rows = []
    unmatched_rows = []
    stats = {}

    # Iterate tables from CSV; match to sheet named NMAquifer_{table_name}
    for _, row in df_csv.iterrows():
        old_table = normalize_name(row.get("table_name"))
        old_cols = split_columns_cell(row.get("columns"))
        sheet_df = sheets_dict.get(old_table)
        matched_count = 0
        unmatched_count = 0
        sheet_rows = len(sheet_df) if sheet_df is not None else 0

        if sheet_df is None:
            for old_col in old_cols:
                unmatched_rows.append({
                    "Old Table Name": old_table,
                    "Old Column Name": old_col,
                    "Reason": "No corresponding sheet (expected 'NMAquifer_{old table name}')"
                })
                unmatched_count += 1
            stats[old_table] = {
                "csv_cols": len(old_cols),
                "sheet_rows": sheet_rows,
                "matched": matched_count,
                "unmatched": unmatched_count,
                "has_sheet": False,
            }
            continue

        # Build lookup from normalized key to full row dict
        lookup = {k: rec for k, rec in zip(
            sheet_df["__key__"],
            sheet_df.to_dict(orient="records")
        )}

        for old_col in old_cols:
            key = make_key(old_col)
            rec = lookup.get(key)
            if rec is None:
                unmatched_rows.append({
                    "Old Table Name": old_table,
                    "Old Column Name": old_col,
                    "Reason": "No matching 'NMAquifer Field Name' (after normalization)"
                })
                unmatched_count += 1
                continue

            nmaq = normalize_name(rec.get("NMAquifer Field Name"))
            o_tab = normalize_name(rec.get("Ocotillo Table Name"))
            o_col = normalize_name(rec.get("Ocotillo Field Name"))
            field_exists = normalize_name(rec.get("Does field exist in Ocotillo?"))
            note = normalize_name(rec.get("Note"))

            matched_rows.append({
                "Old Table Name": old_table,
                "Old Column Name": old_col,
                "NMAquifer Field Name": nmaq,
                "Ocotillo Table Name": o_tab,
                "Ocotillo Field Name": o_col,
                "Does field exist in Ocotillo?": field_exists,
                "Note": note
            })
            matched_count += 1

        stats[old_table] = {
            "csv_cols": len(old_cols),
            "sheet_rows": sheet_rows,
            "matched": matched_count,
            "unmatched": unmatched_count,
            "has_sheet": True,
        }

    # Build DataFrames
    df_matched = pd.DataFrame(matched_rows, columns=[
        "Old Table Name",
        "Old Column Name",
        "NMAquifer Field Name",
        "Ocotillo Table Name",
        "Ocotillo Field Name",
        "Does field exist in Ocotillo?",
        "Note"
    ])
    df_unmatched = pd.DataFrame(unmatched_rows, columns=[
        "Old Table Name",
        "Old Column Name",
        "Reason"
    ])

    # Optional: also save locally as CSV
    df_matched.to_csv("mapping_report_matched.csv", index=False)
    df_unmatched.to_csv("mapping_report_unmatched.csv", index=False)

    # Write to Sheets
    write_df_to_sheet(service, df_matched, MATCHED_SHEET_NAME)
    write_df_to_sheet(service, df_unmatched, UNMATCHED_SHEET_NAME)

    # Stats JSON (local file)
    with open("mapping_report_stats.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    # Build Matrix sheet: columns = Old Table Names, rows = fields for each table
    # Include BOTH matched and unmatched entries.

    matrix_cols = {}
    max_len = 0

    # Get all tables that appear anywhere
    all_tables = set()
    if not df_matched.empty:
        all_tables.update(df_matched["Old Table Name"].dropna().unique())
    if not df_unmatched.empty:
        all_tables.update(df_unmatched["Old Table Name"].dropna().unique())

    # Sort: green headers (has_sheet=True) first, then red (False), each group alphabetical
    def table_sort_key(t):
        has_sheet = stats.get(t, {}).get("has_sheet", False)
        # we want has_sheet=True first → False in first tuple position
        return (not has_sheet, t.lower())

    ordered_tables = sorted(all_tables, key=table_sort_key)

    for table in ordered_tables:
        rows = []

        # Matched rows: use actual status text
        if not df_matched.empty:
            g_m = df_matched[df_matched["Old Table Name"] == table]
            for _, r in g_m.iterrows():
                rows.append(
                    f"{r['Old Column Name']} ({r['Does field exist in Ocotillo?']})"
                )

        # Unmatched rows: label explicitly as unmatched
        if not df_unmatched.empty:
            g_u = df_unmatched[df_unmatched["Old Table Name"] == table]
            for _, r in g_u.iterrows():
                rows.append(
                    f"{r['Old Column Name']} (pending)"
                )

        matrix_cols[table] = rows
        if rows:
            max_len = max(max_len, len(rows))

    # ---------------------------------------
    # Compute status counts for legend (%)
    # ---------------------------------------
    status_counts = {"yes": 0, "no": 0, "N/A": 0, "other": 0}

    # From matched rows
    if not df_matched.empty:
        for _, r in df_matched.iterrows():
            val = normalize_name(r.get("Does field exist in Ocotillo?"))
            low = val.lower()
            if low == "yes":
                status_counts["yes"] += 1
            elif low == "no":
                status_counts["no"] += 1
            elif val.upper() in ("N/A", "NA"):
                status_counts["N/A"] += 1
            else:
                status_counts["other"] += 1

    # From unmatched rows: always "other"
    if not df_unmatched.empty:
        # each unmatched row is a field that didn't map
        status_counts["other"] += len(df_unmatched)

    total_fields = sum(status_counts.values()) or 1  # avoid divide-by-zero

    def pct(n):
        return round(100.0 * n / total_fields, 1)


    # Build matrix DataFrame and write to Sheets (starting at A6)
    if matrix_cols:
        # pad shorter columns
        for table, rows in matrix_cols.items():
            if len(rows) < max_len:
                rows.extend([""] * (max_len - len(rows)))
        df_matrix = pd.DataFrame(matrix_cols)
        df_matrix.to_csv("visual_matrix_for_sheets.csv", index=False)  # optional local

        # Ensure sheet exists & wipe it
        matrix_sheet_id = ensure_sheet(service, MATRIX_SHEET_NAME)
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{MATRIX_SHEET_NAME}'!A:Z"
        ).execute()

        # Write legend in rows 1–5, with percentages in column C
        legend_values = [
            ["Legend"],
            ["Green",
             "migrated",
             f"{pct(status_counts['yes'])}% ({status_counts['yes']}/{total_fields})"],
            ["Yellow",
             "will be migrated",
             f"{pct(status_counts['no'])}% ({status_counts['no']}/{total_fields})"],
            ["Gray",
             "won't be migrated",
             f"{pct(status_counts['N/A'])}% ({status_counts['N/A']}/{total_fields})"],
            ["Light red",
             "pending assessment",
             f"{pct(status_counts['other'])}% ({status_counts['other']}/{total_fields})"],
        ]

        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{MATRIX_SHEET_NAME}'!A1",
            valueInputOption="RAW",
            body={"values": legend_values}
        ).execute()

        # Now write matrix starting at A6
        matrix_values = df_to_values(df_matrix)  # header + data
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{MATRIX_SHEET_NAME}'!A6",
            valueInputOption="RAW",
            body={"values": matrix_values}
        ).execute()

        if APPLY_CONDITIONAL_FORMATTING:
            add_conditional_formatting_for_matrix(service, matrix_sheet_id)
            format_matrix_headers(service, matrix_sheet_id, df_matrix.columns.tolist(), stats)

    else:
        print("No data for Matrix sheet.")



    print(f"✓ Matched rows:   {len(df_matched)}")
    print(f"✓ Unmatched rows: {len(df_unmatched)}")
    print("✓ Sheets updated: Matched, Unmatched, Matrix")
    print("✓ Local files written: mapping_report_matched.csv, mapping_report_unmatched.csv, mapping_report_stats.json, visual_matrix_for_sheets.csv")


if __name__ == "__main__":
    main()
