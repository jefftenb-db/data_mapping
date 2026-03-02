# Synapse to Databricks SQL mapping

Map schema, table, and column names in Synapse SQL to Databricks three-level names (catalog.schema.table) and target column names using an Excel mapping file.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Excel mapping format

The mapping Excel (e.g. `.xlsx`) should have at least these columns:

| Column                 | Description                    |
|------------------------|--------------------------------|
| SOURCE_TABLE_NAME      | Source table name (e.g. SAPECC_WYT3) |
| SOURCE_COLUMN_NAME     | Source column name(s); comma-separated for multi-column mapping |
| TARGET_CATALOG_NAME    | Databricks catalog             |
| TARGET_SCHEMA_NAME     | Databricks schema              |
| TARGET_TABLE_NAME      | Databricks table               |
| TARGET_COLUMN_NAME     | Target column name(s); comma-separated, positional with source |
| SOURCE_SQL_STATEMENT   | (Optional) If present, `map_sql(..., filter_by_sql=True)` uses only rows whose SOURCE_SQL_STATEMENT matches the input SQL |

There is no source catalog column; target always uses `catalog.schema.table`.

## Usage

### Map SQL using an Excel file

```python
from sql_mapper import map_sql, load_mapping

# Input: source SQL and path to your conversion Excel
sql = "SELECT LIFN2 FROM [raw_abdc_finance].[sapecc_wyt3]"
mapping_path = "/path/to/conversion_file.xlsx"

# Use only mapping rows that have this exact SQL (if SOURCE_SQL_STATEMENT exists)
mapped_sql = map_sql(sql, mapping_path, filter_by_sql=True)
# → SELECT other_vend_ref FROM adh_genpro_use2_prd.s_suppliers.partner_functions_hhd

# Use all mapping rows (for any SQL that uses the same table/column names)
mapped_sql = map_sql(sql, mapping_path, filter_by_sql=False)
```

### Map SQL using a DataFrame

```python
import pandas as pd
from sql_mapper import map_sql, load_mapping

df = load_mapping("/path/to/conversion_file.xlsx")
# Or build your own DataFrame with columns:
# SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME, TARGET_CATALOG_NAME, TARGET_SCHEMA_NAME,
# TARGET_TABLE_NAME, TARGET_COLUMN_NAME

mapped_sql = map_sql(sql, df, filter_by_sql=False)
```

### Map SQL with a single mapping row (no Excel)

```python
from sql_mapper import map_sql_from_source_details

mapped_sql = map_sql_from_source_details(
    sql="SELECT LIFN2 FROM [raw_abdc_finance].[sapecc_wyt3]",  # must reference the source column to map it
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
- **Comma-separated columns**  
  - If `SOURCE_COLUMN_NAME` or `TARGET_COLUMN_NAME` is comma-separated, mapping is positional (and extra source columns map to the last target column).

Complex SQL with CTEs and multiple joins is supported; table and column names are replaced wherever they appear, with string literals protected.
