"""
Synapse to Databricks SQL mapping.

Maps schema/table/column names in source SQL to target catalog/schema/table/column
using a mapping table (e.g. from Excel). Handles [schema].[table] and schema.table
references and qualified/unqualified column names.

Optional SQL syntax conversion is driven by a JSON rules file (e.g. remove
"(NOBLOCK)", WITH (NOLOCK), etc.) via load_syntax_rules() and apply_syntax_rules().

Raises MappingError when SOURCE_COLUMN and TARGET_COLUMN have different
numbers of comma-separated values (1:1 mapping required).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd


# Default Excel sheet: 0 = first sheet (works with any workbook)
DEFAULT_SHEET = 0

# Columns expected in mapping file
SOURCE_SCHEMA = "SOURCE_SCHEMA"
SOURCE_TABLE = "SOURCE_TABLE"
SOURCE_COLUMN = "SOURCE_COLUMN"
TARGET_CATALOG = "TARGET_CATALOG"
TARGET_SCHEMA = "TARGET_SCHEMA"
TARGET_TABLE = "TARGET_TABLE"
TARGET_COLUMN = "TARGET_COLUMN"

# Columns expected in input SQL file
INPUT_MAPPING_ID = "MAPPING_ID"
INPUT_DQ_TEST_ID = "DQ_TEST_ID"
INPUT_SQL = "PRIMARY_SQL_QUERY"
INPUT_SECONDARY_SQL = "SECONDARY_SQL_QUERY"
# Literal in SECONDARY_SQL_QUERY that means "no secondary SQL"
NULL_SECONDARY_MARKER = "[NULL]"

# Keys for syntax rules file (JSON)
RULES_KEY = "rules"
RULE_FIND = "find"
RULE_REPLACE = "replace"
RULE_REGEX = "regex"


def load_syntax_rules(source: str | Path) -> list[dict[str, Any]]:
    """
    Load SQL syntax conversion rules from a JSON file.

    File format: {"rules": [{"find": "(NOBLOCK)", "replace": ""}, ...]}
    Add "regex": true to a rule to treat "find" as a regex pattern.

    Args:
        source: Path to .json file.

    Returns:
        List of rule dicts with keys "find", "replace", and optionally "regex".
    """
    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f"Syntax rules file not found: {path}")
    suffix = path.suffix.lower()
    if suffix != ".json":
        raise ValueError(f"Syntax rules file must be .json; got {path.suffix}")
    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    if not isinstance(data, dict) or RULES_KEY not in data:
        raise ValueError(
            f"Syntax rules file must contain a top-level '{RULES_KEY}' key with a list of rules."
        )
    rules = data[RULES_KEY]
    if not isinstance(rules, list):
        raise ValueError(f"'{RULES_KEY}' must be a list of rule objects.")
    out: list[dict[str, Any]] = []
    for i, r in enumerate(rules):
        if not isinstance(r, dict) or RULE_FIND not in r or RULE_REPLACE not in r:
            raise ValueError(
                f"Each rule must be an object with '{RULE_FIND}' and '{RULE_REPLACE}'; "
                f"rule at index {i} is invalid."
            )
        replace = r[RULE_REPLACE]
        out.append(
            {
                RULE_FIND: r[RULE_FIND],
                RULE_REPLACE: replace if isinstance(replace, str) else str(replace),
                RULE_REGEX: bool(r.get(RULE_REGEX, False)),
            }
        )
    return out


def apply_syntax_rules(sql: str, rules: list[dict[str, Any]]) -> str:
    """
    Apply a list of syntax conversion rules to a SQL string.

    Rules are applied in order. Each rule is either a literal find/replace
    or a regex find/replace (when the rule has "regex": true).

    Args:
        sql: SQL string to transform.
        rules: List of rule dicts from load_syntax_rules (find, replace, optional regex).

    Returns:
        SQL string after applying all rules.
    """
    result = sql
    for r in rules:
        find = r[RULE_FIND]
        replace = r[RULE_REPLACE]
        use_regex = r.get(RULE_REGEX, False)
        if use_regex:
            result = re.sub(find, replace, result)
        else:
            result = result.replace(find, replace)
    return result


def load_mapping(
    source: str | Path | pd.DataFrame,
    sheet_name: str | int = DEFAULT_SHEET,
) -> pd.DataFrame:
    """
    Load mapping from Excel file or return DataFrame as-is.

    Args:
        source: Path to Excel file (.xlsx) or a DataFrame.
        sheet_name: Sheet name when source is an Excel path. Ignored if source is DataFrame.

    Returns:
        DataFrame with columns SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN,
        TARGET_CATALOG, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN.
    """
    if isinstance(source, pd.DataFrame):
        return source.copy()
    path = Path(source)
    if path.suffix.lower() not in (".xlsx", ".xls"):
        raise ValueError("Excel source must be .xlsx or .xls")
    df = pd.read_excel(path, sheet_name=sheet_name)
    required = [
        SOURCE_SCHEMA,
        SOURCE_TABLE,
        SOURCE_COLUMN,
        TARGET_CATALOG,
        TARGET_SCHEMA,
        TARGET_TABLE,
        TARGET_COLUMN,
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Mapping missing columns: {missing}")
    return df


def load_input_sql(
    source: str | Path,
    sheet_name: str | int = DEFAULT_SHEET,
) -> pd.DataFrame:
    """
    Load input file with one SQL statement per row.

    Args:
        source: Path to Excel file (.xlsx or .xls).
        sheet_name: Sheet name to read.

    Returns:
        DataFrame with columns MAPPING_ID, DQ_TEST_ID, PRIMARY_SQL_QUERY,
        and optionally SECONDARY_SQL_QUERY.
    """
    path = Path(source)
    if path.suffix.lower() not in (".xlsx", ".xls"):
        raise ValueError("Input SQL source must be .xlsx or .xls")
    df = pd.read_excel(path, sheet_name=sheet_name)
    required = [INPUT_MAPPING_ID, INPUT_DQ_TEST_ID, INPUT_SQL]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Input SQL file missing columns: {missing}")
    return df


def _build_sql_from_input_row(row: pd.Series) -> str:
    """
    Build the full SQL string from a row: PRIMARY_SQL_QUERY, optionally
    concatenated with SECONDARY_SQL_QUERY (ignored if missing or [NULL]).
    Inserts /* BEGIN_SECONDARY_SQL */ between primary and secondary for later splitting.
    """
    primary = row[INPUT_SQL]
    if pd.isna(primary):
        primary = ""
    else:
        primary = str(primary).strip()
    secondary_raw = row.get(INPUT_SECONDARY_SQL)
    if secondary_raw is None or pd.isna(secondary_raw):
        return primary
    secondary = str(secondary_raw).strip()
    if not secondary or secondary.upper() == NULL_SECONDARY_MARKER.upper():
        return primary
    return f"{primary} /* BEGIN_SECONDARY_SQL */ {secondary}"


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


class MappingError(Exception):
    """Raised when column mapping is invalid (e.g. source and target lengths differ)."""


def _build_column_map(df: pd.DataFrame) -> dict[tuple[str, str], str]:
    """
    Build (source_table_lower, source_column) -> target_column.
    Handles comma-separated SOURCE_COLUMN and TARGET_COLUMN (positional).
    Requires 1:1 mapping: len(source columns) must equal len(target columns); raises MappingError otherwise.
    """
    out: dict[tuple[str, str], str] = {}
    for idx, row in df.iterrows():
        table_key = str(row[SOURCE_TABLE]).strip().lower()
        src_cols = [s.strip() for s in str(row[SOURCE_COLUMN]).split(",") if s.strip()]
        tgt_cols = [s.strip() for s in str(row[TARGET_COLUMN]).split(",") if s.strip()]
        if not src_cols or not tgt_cols:
            continue
        if len(src_cols) != len(tgt_cols):
            raise MappingError(
                f"Column mapping must be 1:1. Row (table={row[SOURCE_TABLE]!r}): "
                f"SOURCE_COLUMN has {len(src_cols)} value(s) {src_cols!r}, "
                f"TARGET_COLUMN has {len(tgt_cols)} value(s) {tgt_cols!r}. "
                "Use the same number of comma-separated source and target columns."
            )
        for i in range(len(src_cols)):
            sc, tc = src_cols[i].strip(), tgt_cols[i].strip()
            if sc and tc:
                out[(table_key, sc.lower())] = tc
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
    sheet_name: str | int = DEFAULT_SHEET,
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

    Returns:
        The SQL string with table and column names replaced.
    """
    df = load_mapping(mapping, sheet_name=sheet_name)

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
                SOURCE_SCHEMA: "",  # optional for single-row convenience
                SOURCE_TABLE: source_table_name,
                SOURCE_COLUMN: source_column_name,
                TARGET_CATALOG: target_catalog_name,
                TARGET_SCHEMA: target_schema_name,
                TARGET_TABLE: target_table_name,
                TARGET_COLUMN: target_column_name,
            }
        ]
    )
    return map_sql(sql, df)


def process_input_file(
    input_path: str | Path,
    mapping_path: str | Path,
    output_csv_path: str | Path,
    output_sql_path: str | Path,
    *,
    input_sheet: str | int = DEFAULT_SHEET,
    mapping_sheet: str | int = DEFAULT_SHEET,
    syntax_rules_path: str | Path | None = None,
) -> None:
    """
    Load SQL statements from the input file, map each using the mapping file,
    and write two outputs: a CSV (IDs + mapped SQL) and a .sql file (statements only, semicolon-terminated).

    Optionally applies syntax conversion rules from a JSON or YAML file (e.g. remove (NOBLOCK)).

    Args:
        input_path: Path to Excel file with columns MAPPING_ID, DQ_TEST_ID, PRIMARY_SQL_QUERY,
            and optionally SECONDARY_SQL_QUERY (concatenated after primary with /* BEGIN_SECONDARY_SQL */; use [NULL] to omit).
        mapping_path: Path to Excel mapping file with SOURCE_* and TARGET_* columns.
        output_csv_path: Path for CSV output (MAPPING_ID, DQ_TEST_ID, MAPPED_SQL).
        output_sql_path: Path for .sql output (one statement per line, each prefixed with /* MAPPING_ID,DQ_TEST_ID */ and ending with ;).
        input_sheet: Sheet name in input file.
        mapping_sheet: Sheet name in mapping file.
        syntax_rules_path: Optional path to JSON file defining find/replace syntax rules.
    """
    input_df = load_input_sql(input_path, sheet_name=input_sheet)
    mapping_df = load_mapping(mapping_path, sheet_name=mapping_sheet)
    syntax_rules: list[dict[str, Any]] = []
    if syntax_rules_path is not None:
        syntax_rules = load_syntax_rules(syntax_rules_path)

    rows = []
    sql_statements = []
    for _, row in input_df.iterrows():
        mapping_id = row[INPUT_MAPPING_ID]
        dq_test_id = row[INPUT_DQ_TEST_ID]
        sql = _build_sql_from_input_row(row)
        if not sql:
            mapped = ""
        else:
            # Normalize to single line (collapse tabs, carriage returns, newlines)
            sql_normalized = _normalize_sql(sql)
            mapped = map_sql(sql_normalized, mapping_df, sheet_name=mapping_sheet)
            if syntax_rules:
                mapped = apply_syntax_rules(mapped, syntax_rules)
            mapped = _normalize_sql(mapped)
        rows.append(
            {
                INPUT_MAPPING_ID: mapping_id,
                INPUT_DQ_TEST_ID: dq_test_id,
                "MAPPED_SQL": mapped,
            }
        )
        # Ensure statement ends with semicolon for .sql file; prepend ID comment
        stmt = mapped.rstrip()
        if stmt and not stmt.endswith(";"):
            stmt += ";"
        id_comment = "/* {},{} */".format(
            "" if pd.isna(mapping_id) else mapping_id,
            "" if pd.isna(dq_test_id) else dq_test_id,
        )
        sql_statements.append(f"{id_comment} {stmt}")

    out_csv = Path(output_csv_path)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_csv, index=False)

    out_sql = Path(output_sql_path)
    out_sql.parent.mkdir(parents=True, exist_ok=True)
    out_sql.write_text("\n".join(sql_statements), encoding="utf-8")


# # Default file names for batch run
# DEFAULT_INPUT_SQL_FILE = "mapping_input_sql.xlsx"
# DEFAULT_MAPPING_FILE = "mapping_master.xlsx"
# DEFAULT_OUTPUT_CSV = "mapped_output.csv"
# DEFAULT_OUTPUT_SQL = "mapped_output.sql"


# def main(
#     input_path: str | Path = DEFAULT_INPUT_SQL_FILE,
#     mapping_path: str | Path = DEFAULT_MAPPING_FILE,
#     output_csv: str | Path = DEFAULT_OUTPUT_CSV,
#     output_sql: str | Path = DEFAULT_OUTPUT_SQL,
# ) -> None:
#     """Run batch mapping: input SQL file + mapping file -> CSV and .sql outputs."""
#     process_input_file(input_path, mapping_path, output_csv, output_sql)


# if __name__ == "__main__":
#     main()
