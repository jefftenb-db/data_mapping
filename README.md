# Synapse to Databricks SQL mapping

Map schema, table, and column names in Synapse SQL to Databricks three-level names (catalog.schema.table) and target column names using Excel mapping and input files.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## File formats

### Input SQL file (`mapping_input_sql.xlsx`)

One SQL statement per row. Required columns:

| Column            | Description                    |
|-------------------|--------------------------------|
| MAPPING_ID        | ID for audit (included in CSV output) |
| DQ_TEST_ID        | ID for audit (included in CSV output) |
| PRIMARY_SQL_QUERY | Source SQL statement to map   |

### Mapping file (`mapping_master.xlsx`)

Source → target name lookup. Required columns:

| Column         | Description                    |
|----------------|--------------------------------|
| SOURCE_SCHEMA  | Source schema                  |
| SOURCE_TABLE   | Source table name              |
| SOURCE_COLUMN  | Source column name(s); comma-separated for multi-column mapping |
| TARGET_CATALOG | Databricks catalog             |
| TARGET_SCHEMA  | Databricks schema              |
| TARGET_TABLE   | Databricks table               |
| TARGET_COLUMN  | Target column name(s); comma-separated, positional with source  |

**Column mapping must be 1:1:** when `SOURCE_COLUMN` or `TARGET_COLUMN` contains comma-separated values, the number of source and target columns must match. Otherwise `MappingError` is raised.

### Syntax rules file (optional, JSON)

Optional find/replace rules applied to SQL after mapping (e.g. remove Synapse hints, fix dialect differences). Copy `sql_syntax_rules.example.json` to `sql_syntax_rules.json` and edit.

| Key       | Description |
|-----------|--------------|
| `find`    | String or regex pattern to find |
| `replace` | Replacement (use `""` to remove) |
| `regex`   | Optional; if `true`, `find` is a regex and `replace` may use `\1`, `\2` for capture groups |

Example:

```json
{
  "rules": [
    { "find": "(NOBLOCK)", "replace": "" },
    { "find": "TABLESAMPLE\\s*\\(\\s*(\\d+)\\s*PERCENTAGE\\s*\\)", "replace": "TABLESAMPLE (\\1 PERCENT)", "regex": true }
  ]
}
```

## Usage

### Batch run (input file → CSV + .sql)

From the project directory, with `mapping_input_sql.xlsx` and `mapping_master.xlsx` in place:

```bash
python -m sql_mapper
```

This produces:

- **mapped_output.csv** — columns `MAPPING_ID`, `DQ_TEST_ID`, `MAPPED_SQL` (for audit).
- **mapped_output.sql** — one mapped statement per block, each terminated with a semicolon.

Custom paths (optionally with syntax rules):

```python
from sql_mapper import process_input_file

process_input_file(
    "mapping_input_sql.xlsx",
    "mapping_master.xlsx",
    "mapped_output.csv",
    "mapped_output.sql",
    syntax_rules_path="sql_syntax_rules.json",  # optional
)
```

### Apply syntax rules only (no mapping)

```python
from sql_mapper import load_syntax_rules, apply_syntax_rules

rules = load_syntax_rules("sql_syntax_rules.json")
cleaned_sql = apply_syntax_rules(sql, rules)
```

### Map a single SQL statement

```python
from sql_mapper import map_sql, load_mapping

sql = "SELECT LIFN2 FROM [raw_abdc_finance].[sapecc_wyt3]"
mapping_path = "mapping_master.xlsx"
mapped_sql = map_sql(sql, mapping_path)
```

### Map SQL with a single mapping row (no Excel)

```python
from sql_mapper import map_sql_from_source_details

mapped_sql = map_sql_from_source_details(
    sql="SELECT LIFN2 FROM [raw_abdc_finance].[sapecc_wyt3]",
    source_table_name="SAPECC_WYT3",
    source_column_name="LIFN2",
    target_catalog_name="adh_genpro_use2_prd",
    target_schema_name="s_suppliers",
    target_table_name="partner_functions_hhd",
    target_column_name="other_vend_ref",
)
```

## Behavior

- **Table references**  
  - `[schema].[table]` and `schema.table` are replaced with `catalog.schema.table` using the mapping (table name match is case-insensitive).
- **Column references**  
  - Qualified (`table.column` or `alias.column`) and unqualified column names are replaced when they match the mapping. Unqualified replacement is whole-word only.
- **String literals**  
  - Content inside single-quoted strings is not changed (e.g. `'BILL_NUM || BILL_ITEM' AS PRIMARY_KEY_NAME` stays as-is).
- **Syntax rules (optional)**  
  - If `syntax_rules_path` is provided, rules are loaded from the JSON file and applied to each mapped SQL statement (after table/column mapping). Rules run in order; use `"regex": true` for regex patterns and `\1`, `\2` in `replace` for capture groups.
- **Comma-separated columns**  
  - If `SOURCE_COLUMN` or `TARGET_COLUMN` is comma-separated, the counts must match (1:1). Otherwise `MappingError` is raised.

Complex SQL with CTEs and multiple joins is supported; table and column names are replaced wherever they appear, with string literals protected.
