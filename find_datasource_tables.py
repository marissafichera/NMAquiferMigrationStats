#!/usr/bin/env python3
import sys, csv
from pathlib import Path
import pyodbc

# ---- CONFIG: edit these for your environment ----
SERVER   = "SQL Server"
DATABASE = "NM_Aquifer_Testing_DB"
ODBC_DRIVER = "{ODBC Driver 17 for SQL Server}"  # or "{ODBC Driver 18 for SQL Server}"
TRUSTED_CONNECTION = True  # set False and add UID/PWD if needed
UID = ""
PWD = ""
SCHEMA = "dbo"
# -------------------------------------------------

def qident(name: str) -> str:
    """Bracket-quote an identifier and escape closing bracket."""
    return f"[{name.replace(']', ']]')}]"

def get_conn():
    parts = [f"Driver={ODBC_DRIVER}", f"Server={SERVER}", f"Database={DATABASE}"]
    if TRUSTED_CONNECTION:
        parts.append("Trusted_Connection=yes")
    else:
        parts += [f"UID={UID}", f"PWD={PWD}"]
    return pyodbc.connect(";".join(parts))

def read_csv_values(path: Path):
    vals = []
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if "DataSource" not in reader.fieldnames:
            raise ValueError("Input CSV must have a 'DataSource' column.")
        for row in reader:
            v = (row["DataSource"] or "").strip()
            if v:
                vals.append(v)
    # de-dup while preserving order
    seen, out = set(), []
    for v in vals:
        if v.lower() not in seen:
            seen.add(v.lower())
            out.append(v)
    return out

def discover_objects(cur):
    """Return list of (schema, object, has_ds, has_ma) for dbo tables/views."""
    cur.execute("""
        SELECT
          s.name AS schema_name,
          o.name AS object_name,
          MAX(CASE WHEN c.name = 'DataSource'      THEN 1 ELSE 0 END) AS has_ds,
          MAX(CASE WHEN c.name = 'MeasuringAgency' THEN 1 ELSE 0 END) AS has_ma
        FROM sys.objects o
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        JOIN sys.columns c ON c.object_id = o.object_id
        WHERE s.name = ?
          AND o.type IN ('U','V')
          AND c.name IN ('DataSource','MeasuringAgency')
        GROUP BY s.name, o.name
        ORDER BY o.name
    """, (SCHEMA,))
    return [(r.schema_name, r.object_name, int(r.has_ds), int(r.has_ma)) for r in cur.fetchall()]

def main(in_csv: Path, out_csv: Path):
    values = read_csv_values(in_csv)
    if not values:
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(["DataSource","DataTable"])
        print("No DataSource values in input; wrote empty output with header.")
        return

    conn = get_conn()
    conn.timeout = 0  # no query timeout
    pairs = []  # (DataSource, DataTable)

    with conn.cursor() as cur:
        objs = discover_objects(cur)
        if not objs:
            with open(out_csv, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(["DataSource","DataTable"])
            print("No dbo tables/views with DataSource/MeasuringAgency found.")
            return

        # Prebuild per-object predicate & parameter layout
        obj_checks = []
        for sch, obj, has_ds, has_ma in objs:
            where_clauses = []
            params_layout = []  # 'ds' or 'ma' per placeholder position
            if has_ds:
                where_clauses.append(
                    "LTRIM(RTRIM(CONVERT(NVARCHAR(4000), t.[DataSource]))) "
                    "COLLATE SQL_Latin1_General_CP1_CI_AS = LTRIM(RTRIM(?)) COLLATE SQL_Latin1_General_CP1_CI_AS"
                )
                params_layout.append('v')
            if has_ma:
                where_clauses.append(
                    "LTRIM(RTRIM(CONVERT(NVARCHAR(4000), t.[MeasuringAgency]))) "
                    "COLLATE SQL_Latin1_General_CP1_CI_AS = LTRIM(RTRIM(?)) COLLATE SQL_Latin1_General_CP1_CI_AS"
                )
                params_layout.append('v')

            sql = (
                f"SELECT TOP (1) 1 "
                f"FROM {qident(sch)}.{qident(obj)} AS t "
                f"WHERE " + " OR ".join(where_clauses)
            )
            obj_checks.append((sch, obj, sql, params_layout))

        # For each input value, probe each object
        for v in values:
            for sch, obj, sql, params_layout in obj_checks:
                try:
                    params = tuple(v for _ in params_layout)
                    cur.execute(sql, params)
                    row = cur.fetchone()
                    if row:
                        pairs.append((v, f"{sch}.{obj}"))
                except Exception as e:
                    # Skip problematic objects but keep going
                    print(f"Skipped {sch}.{obj}: {e}")

    # de-dup final pairs
    seen = set()
    uniq_pairs = []
    for ds, dt in pairs:
        key = (ds.lower(), dt.lower())
        if key not in seen:
            seen.add(key)
            uniq_pairs.append((ds, dt))

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["DataSource","DataTable"])
        w.writerows(uniq_pairs)

    print(f"Wrote {len(uniq_pairs)} rows to {out_csv}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python find_datasource_tables.py INPUT.csv OUTPUT.csv")
        sys.exit(1)
    main(Path(sys.argv[1]).expanduser(), Path(sys.argv[2]).expanduser())
