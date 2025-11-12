#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build a mapping report of old -> new fields using:
- CSV   : "NM_Aquifer_Testing_DB_tables_cols.csv" with columns:
          table_name, columns  (columns = comma-separated list of old columns)
- Excel/Sheets export : "NMAquifer_mapping.xlsx" where each sheet name is
          "NMAquifer_{old table name}".
          Relevant columns per sheet:
            - "NMAquifer Field Name"
            - "Ocotillo Table Name"
            - "Ocotillo Field Name"
            - "Does field exist in Ocotillo?"
            - "Note"

Matching is done using a normalized key that:
- lowercases
- strips all non-alphanumeric characters

So e.g. "Well_ID", "well id", "WELL-ID" -> "wellid".

Outputs:
  - mapping_report_matched.csv
  - mapping_report_unmatched.csv
  - mapping_report.xlsx  (Matched / Unmatched sheets)
  - mapping_report_matched.json
  - mapping_report_stats.json
  - visual_matrix_for_sheets.csv  (for the colored chart)
"""

import argparse
import pandas as pd
from pathlib import Path
import sys
import json
import re

REQ_SHEET_COLS = [
    "NMAquifer Field Name",
    "Ocotillo Table Name",
    "Ocotillo Field Name",
    "Does field exist in Ocotillo?",
    # "Note" is optional; we'll add if missing
]

SHEET_PREFIX = "NMAquifer_"  # sheet names look like NMAquifer_{old table name}

def normalize_name(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def make_key(x):
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

def split_columns_cell(cell):
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

def load_excel_sheets(xlsx_path):
    """
    Return dict of old_table_name -> DataFrame.
    Each sheet is expected to be named 'NMAquifer_{old table name}'.
    We'll strip the prefix and use the remainder as the key.
    """
    xls = pd.ExcelFile(xlsx_path)
    sheets = {}
    for sheet_name in xls.sheet_names:
        old_table = strip_prefix_case_insensitive(sheet_name, SHEET_PREFIX)
        df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
        for col in REQ_SHEET_COLS:
            if col not in df.columns:
                df[col] = pd.NA
        if "Note" not in df.columns:
            df["Note"] = pd.NA
        df["__key__"] = df["NMAquifer Field Name"].map(make_key)
        sheets[normalize_name(old_table)] = df
    return sheets

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv",  default="NM_Aquifer_Testing_DB_tables_cols.csv", help="Path to NM_Aquifer_Testing_DB_tables_cols.csv")
    ap.add_argument("--xlsx", default="NMAquifer_mapping.xlsx", help="Path to NMAquifer_mapping.xlsx (exported from Google Sheets)")
    ap.add_argument("--out",  default="mapping_report.xlsx", help="Output Excel file (default: mapping_report.xlsx)")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    xlsx_path = Path(args.xlsx)

    if not csv_path.exists():
        sys.exit(f"CSV not found: {csv_path}")
    if not xlsx_path.exists():
        sys.exit(f"Excel not found: {xlsx_path}")

    try:
        df_csv = pd.read_csv(csv_path, dtype=str)
    except Exception as e:
        sys.exit(f"Failed to read CSV: {e}")

    if "table_name" not in df_csv.columns or "columns" not in df_csv.columns:
        sys.exit("CSV must contain 'table_name' and 'columns' columns.")

    try:
        sheets = load_excel_sheets(xlsx_path)
    except Exception as e:
        sys.exit(f"Failed to read Excel: {e}")

    matched_rows = []
    unmatched_rows = []
    stats = {}

    # Iterate tables from CSV; find sheet named "NMAquifer_{table_name}"
    for _, row in df_csv.iterrows():
        old_table = normalize_name(row.get("table_name"))
        old_cols = split_columns_cell(row.get("columns"))
        sheet_df = sheets.get(old_table)
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

        lookup = {k: rec for k, rec in zip(sheet_df["__key__"], sheet_df.to_dict(orient="records"))}

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

    # Write CSVs
    df_matched.to_csv("mapping_report_matched.csv", index=False)
    df_unmatched.to_csv("mapping_report_unmatched.csv", index=False)

    # Write Excel with two sheets
    with pd.ExcelWriter(args.out, engine="openpyxl") as writer:
        df_matched.to_excel(writer, sheet_name="Matched", index=False)
        df_unmatched.to_excel(writer, sheet_name="Unmatched", index=False)

    # Compact JSON per old table of matched Ocotillo fields (only if we have any)
    mapping = {}
    if not df_matched.empty:
        mapping = (
            df_matched.groupby(["Old Table Name"])["Ocotillo Field Name"]
            .apply(list).sort_index().to_dict()
        )

    with open("mapping_report_matched.json", "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2)

    # Dump stats to help debug coverage
    with open("mapping_report_stats.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    # Build a visual matrix: columns = Old Table Names, rows = fields,
    # each cell string is "Old Column Name (Does field exist in Ocotillo)"
    matrix_cols = {}
    max_len = 0
    for table, group in df_matched.groupby("Old Table Name"):
        # keep original row order within each table
        rows = [
            f"{r['Old Column Name']} ({r['Does field exist in Ocotillo?']})"
            for _, r in group.iterrows()
        ]
        matrix_cols[table] = rows
        if len(rows) > max_len:
            max_len = len(rows)

    # pad shorter columns with empty strings
    for table, rows in matrix_cols.items():
        if len(rows) < max_len:
            rows.extend([""] * (max_len - len(rows)))

    if matrix_cols:
        df_matrix = pd.DataFrame(matrix_cols)
        df_matrix.to_csv("visual_matrix_for_sheets.csv", index=False)

    print(f"✓ Wrote mapping_report_matched.csv ({len(df_matched)} rows)")
    print(f"✓ Wrote mapping_report_unmatched.csv ({len(df_unmatched)} rows)")
    print(f"✓ Wrote {args.out} with sheets: Matched, Unmatched")
    print("✓ Wrote mapping_report_matched.json (per-table list of Ocotillo fields)")
    print("✓ Wrote mapping_report_stats.json (per-table coverage summary)")
    if matrix_cols:
        print("✓ Wrote visual_matrix_for_sheets.csv (Old Table Names as columns; fields per column, with status)")

if __name__ == "__main__":
    main()
