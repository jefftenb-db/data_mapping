from sql_mapper import map_sql, load_mapping, load_syntax_rules, apply_syntax_rules, process_input_file  # , main

# Option 1: Batch process using input file + mapping file (produces CSV + .sql)
# Uses mapping_input_sql.xlsx and mapping_master.xlsx by default:
# main()

# Or with custom paths:
# process_input_file(
#     input_path="mapping_input_sql.xlsx", #input file with sql statements and mapping id and dq test id
#     mapping_path="mapping_master.xlsx", #mapping file with source and target names
#     output_csv_path="mapped_output.csv", #output file with mapped sql statements and mapping id and dq test id
#     output_sql_path="transpiler_input/mapped_output.sql", #output file with mapped sql statements, to be used in transpiler
#     syntax_rules_path="sql_syntax_rules.json" #optional syntax rules file
# )

# Option 2: Map a single SQL statement with mapping file
sql = """
SELECT Distinct KEY_BILL_NUM_BILL_ITEM, ORD_LINE_NBR,'26' AS MAPPING_ID FROM processed_abdc_operations.sap_billing tablesample(10 percent) WITH (NOLOCK) WHERE Invc_Date &gt;= DATEADD(d, -365, CAST(Getdate() AS DATE)) AND sales_org_nbr IN ('1000', '1200', '2000')and  Invc_Type_Cd in ('zf2','zf2d','zf2f')
"""
#sql = "SELECT col FROM [schema].[table]"

mapping_path = "mapping_master.xlsx"
syntax_rules_path = "sql_syntax_rules.json"
mapped = map_sql(sql.strip(), mapping_path)
syntax_rules = load_syntax_rules(syntax_rules_path) if syntax_rules_path else []
if syntax_rules:
    mapped = apply_syntax_rules(mapped, syntax_rules)
print(mapped)