"""
Synapse to Databricks SQL mapping.

Maps schema/table/column names in source SQL to target catalog/schema/table/column
using a mapping table (e.g. from Excel). Handles [schema].[table] and schema.table
references and qualified/unqualified column names.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd


# Default Excel sheet name if not provided
DEFAULT_SHEET = "result"

# Columns expected in mapping
SOURCE_TABLE = "SOURCE_TABLE_NAME"
SOURCE_COLUMN = "SOURCE_COLUMN_NAME"
TARGET_CATALOG = "TARGET_CATALOG_NAME"
TARGET_SCHEMA = "TARGET_SCHEMA_NAME"
TARGET_TABLE = "TARGET_TABLE_NAME"
TARGET_COLUMN = "TARGET_COLUMN_NAME"
SOURCE_SQL = "SOURCE_SQL_STATEMENT"


def load_mapping(
    source: str | Path | pd.DataFrame,
    sheet_name: str = DEFAULT_SHEET,
) -> pd.DataFrame:
    """
    Load mapping from Excel file or return DataFrame as-is.

    Args:
        source: Path to Excel file (.xlsx) or a DataFrame.
        sheet_name: Sheet name when source is an Excel path. Ignored if source is DataFrame.

    Returns:
        DataFrame with columns SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME,
        TARGET_CATALOG_NAME, TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, TARGET_COLUMN_NAME,
        and optionally SOURCE_SQL_STATEMENT.
    """
    if isinstance(source, pd.DataFrame):
        return source.copy()
    path = Path(source)
    if not path.suffix.lower() in (".xlsx", ".xls"):
        raise ValueError("Excel source must be .xlsx or .xls")
    df = pd.read_excel(path, sheet_name=sheet_name)
    required = [SOURCE_TABLE, SOURCE_COLUMN, TARGET_CATALOG, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Mapping missing columns: {missing}")
    return df


def _normalize_sql(sql: Any) -> str:
    """Normalize SQL for comparison (collapse whitespace)."""
    if sql is None or (isinstance(sql, float) and pd.isna(sql)):
        return ""
    s = str(sql).strip()
    return " ".join(s.split()) if s else ""


def _build_table_map(df: pd.DataFrame) -> dict[str, tuple[str, str, str]]:
    """Build source table (lower) -> (catalog, schema, table). First occurrence wins."""
    out: dict[str, tuple[str, str, str]] = {}
    for _, row in df.drop_duplicates(SOURCE_TABLE).iterrows():
        key = str(row[SOURCE_TABLE]).strip().lower()
        if key in out:
            continue
        out[key] = (
            str(row[TARGET_CATALOG]).strip(),
            str(row[TARGET_SCHEMA]).strip(),
            str(row[TARGET_TABLE]).strip(),
        )
    return out


def _build_column_map(df: pd.DataFrame) -> dict[tuple[str, str], str]:
    """
    Build (source_table_lower, source_column) -> target_column.
    Handles comma-separated SOURCE_COLUMN_NAME and TARGET_COLUMN_NAME (positional).
    When lengths differ, extra source columns map to the last target column.
    """
    out: dict[tuple[str, str], str] = {}
    for _, row in df.iterrows():
        table_key = str(row[SOURCE_TABLE]).strip().lower()
        src_cols = [s.strip() for s in str(row[SOURCE_COLUMN]).split(",") if s.strip()]
        tgt_cols = [s.strip() for s in str(row[TARGET_COLUMN]).split(",") if s.strip()]
        if not src_cols or not tgt_cols:
            continue
        if len(src_cols) >= 1 and len(tgt_cols) >= 1:
            # Positional: src_cols[i] -> tgt_cols[min(i, len(tgt_cols)-1)]
            for i, sc in enumerate(src_cols):
                if not sc:
                    continue
                tc = tgt_cols[min(i, len(tgt_cols) - 1)]
                if tc:
                    out[(table_key, sc.lower())] = tc
        else:
            src_one = str(row[SOURCE_COLUMN]).strip()
            tgt_one = str(row[TARGET_COLUMN]).strip()
            if src_one and tgt_one:
                out[(table_key, src_one.lower())] = tgt_one
    return out


def _mask_string_literals(sql: str) -> tuple[str, list[str]]:
    """
    Replace single-quoted string literals with placeholders. Returns (modified_sql, list of original strings).
    Handles escaped '' inside strings.
    """
    literals: list[str] = []
    result = []
    i = 0
    while i < len(sql):
        if sql[i] == "'" and (i == 0 or sql[i - 1] != "'"):
            # Start of string literal
            start = i
            i += 1
            while i < len(sql):
                if sql[i] == "'":
                    if i + 1 < len(sql) and sql[i + 1] == "'":
                        i += 2  # escaped quote
                        continue
                    break
                i += 1
            i += 1  # closing quote
            literals.append(sql[start:i])
            result.append(f"\x00\x00{len(literals)-1}\x00\x00")
            continue
        result.append(sql[i])
        i += 1
    return "".join(result), literals


def _unmask_string_literals(sql: str, literals: list[str]) -> str:
    """Restore masked string literals."""
    for idx, lit in enumerate(literals):
        sql = sql.replace(f"\x00\x00{idx}\x00\x00", lit)
    return sql


def _replace_table_refs(sql: str, table_map: dict[str, tuple[str, str, str]]) -> str:
    """
    Replace Synapse-style table references with Databricks catalog.schema.table.
    Handles [schema].[table] and schema.table (unquoted). Case-insensitive table match.
    """
    result = sql

    # 1) Bracket notation: [schema].[table]
    def bracket_repl(m: re.Match) -> str:
        schema_part, table_part = m.group(1), m.group(2)
        table_lower = table_part.lower()
        if table_lower in table_map:
            cat, sch, tbl = table_map[table_lower]
            return f"{cat}.{sch}.{tbl}"
        return m.group(0)

    result = re.sub(r"\[([^\]]+)\]\.\[([^\]]+)\]", bracket_repl, result, flags=re.IGNORECASE)

    # 2) Dot notation: schema.table (word.word) - avoid matching number.number
    def dot_repl(m: re.Match) -> str:
        schema_part, table_part = m.group(1), m.group(2)
        table_lower = table_part.lower()
        if table_lower in table_map:
            cat, sch, tbl = table_map[table_lower]
            return f"{cat}.{sch}.{tbl}"
        return m.group(0)

    # Match identifier.identifier (identifiers: letters, digits, underscore; not starting with digit)
    result = re.sub(
        r"\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b",
        dot_repl,
        result,
    )

    return result


def _replace_column_refs(
    sql: str,
    column_map: dict[tuple[str, str], str],
    table_map: dict[str, tuple[str, str, str]],
) -> str:
    """
    Replace column references: qualified (table.col -> table.target_col) and unqualified (col -> target_col).
    Uses table_map to know which table names (source or target) to consider for qualification.
    """
    result = sql
    # Build set of source and target table names (lower) that participate in column mapping
    table_names: set[str] = set()
    for (tbl, _), _ in column_map.items():
        table_names.add(tbl)
        if tbl in table_map:
            _, _, tgt_tbl = table_map[tbl]
            table_names.add(tgt_tbl.lower())

    # 1) Qualified: "table.col" or "alias.col" -> replace col when table/alias is in our set
    def qualified_repl(m: re.Match) -> str:
        qualifier = m.group(1)
        col = m.group(2)
        qual_lower = qualifier.lower()
        col_lower = col.lower()
        for (tbl, src_col), tgt_col in column_map.items():
            if src_col != col_lower:
                continue
            if qual_lower == tbl or (tbl in table_map and qual_lower == table_map[tbl][2].lower()):
                return f"{qualifier}.{tgt_col}"
        return m.group(0)

    result = re.sub(
        r"\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b",
        qualified_repl,
        result,
    )

    # 2) Unqualified: whole-word column name. Replace only when it's a known source column.
    # Sort by length descending so longer names are replaced first (e.g. col_a before col)
    replacements = []
    for (tbl, src_col), tgt_col in column_map.items():
        if src_col == tgt_col.lower():
            continue
        replacements.append((src_col, tgt_col))
    # Dedupe by src_col (first target wins)
    seen: set[str] = set()
    unique_repl: list[tuple[str, str]] = []
    for src, tgt in replacements:
        if src not in seen:
            seen.add(src)
            unique_repl.append((src, tgt))
    for src_col, tgt_col in sorted(unique_repl, key=lambda x: -len(x[0])):
        # Word boundary: not inside another identifier
        pattern = r"\b" + re.escape(src_col) + r"\b"
        result = re.sub(pattern, lambda m: tgt_col, result, flags=re.IGNORECASE)

    return result


def map_sql(
    sql: str,
    mapping: str | Path | pd.DataFrame,
    *,
    sheet_name: str = DEFAULT_SHEET,
    filter_by_sql: bool = True,
) -> str:
    """
    Map a source (Synapse) SQL statement to target (Databricks) using the mapping.

    Table references [schema].[table] or schema.table are replaced with
    catalog.schema.table. Column names are replaced according to the mapping;
    qualified column references (table.column) are updated when the table
    is in the mapping.

    Args:
        sql: The source SQL statement to convert.
        mapping: Path to Excel file or a DataFrame with mapping columns.
        sheet_name: Sheet name when mapping is an Excel path.
        filter_by_sql: If True and mapping has SOURCE_SQL_STATEMENT, only use rows
            where SOURCE_SQL_STATEMENT matches the given sql (after normalizing whitespace).
            If False, use all rows in the mapping.

    Returns:
        The SQL string with table and column names replaced.
    """
    df = load_mapping(mapping, sheet_name=sheet_name)

    if filter_by_sql and SOURCE_SQL in df.columns:
        normalized = _normalize_sql(sql)
        if normalized:
            has_sql = df[SOURCE_SQL].notna() & (df[SOURCE_SQL].astype(str).str.strip() != "")
            mask = has_sql & (df[SOURCE_SQL].astype(str).apply(_normalize_sql) == normalized)
            if mask.any():
                df = df.loc[mask].copy()

    table_map = _build_table_map(df)
    column_map = _build_column_map(df)

    if not table_map and not column_map:
        return sql

    # Protect string literals from replacement (e.g. 'BILL_NUM || ...' in AS clauses)
    sql_work, literals = _mask_string_literals(sql)
    result = _replace_table_refs(sql_work, table_map)
    result = _replace_column_refs(result, column_map, table_map)
    return _unmask_string_literals(result, literals)


def map_sql_from_source_details(
    sql: str,
    source_table_name: str,
    source_column_name: str,
    target_catalog_name: str,
    target_schema_name: str,
    target_table_name: str,
    target_column_name: str,
) -> str:
    """
    Map a single SQL statement using one row of mapping (convenience when no Excel).

    Args:
        sql: Source SQL to convert.
        source_table_name: Source table name (e.g. SAPECC_WYT3).
        source_column_name: Source column name (e.g. LIFN2).
        target_catalog_name: Target catalog (e.g. adh_genpro_use2_prd).
        target_schema_name: Target schema (e.g. s_suppliers).
        target_table_name: Target table (e.g. partner_functions_hhd).
        target_column_name: Target column (e.g. other_vend_ref).

    Returns:
        Mapped SQL string.
    """
    df = pd.DataFrame(
        [
            {
                SOURCE_TABLE: source_table_name,
                SOURCE_COLUMN: source_column_name,
                TARGET_CATALOG: target_catalog_name,
                TARGET_SCHEMA: target_schema_name,
                TARGET_TABLE: target_table_name,
                TARGET_COLUMN: target_column_name,
            }
        ]
    )
    return map_sql(sql, df, filter_by_sql=False)
