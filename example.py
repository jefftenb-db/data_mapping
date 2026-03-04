from sql_mapper import map_sql, load_mapping, process_input_file #, main

# Option 1: Batch process using input file + mapping file (produces CSV + .sql)
# Uses mapping_input_sql.xlsx and mapping_master.xlsx by default:
# main()
# Or with custom paths:
process_input_file(
    "mapping_input_sql.xlsx", #input file with sql statements and mapping id and dq test id
    "mapping_master.xlsx", #mapping file with source and target names
    "mapped_output.csv", #output file with mapped sql statements and mapping id and dq test id
    "transpiler_input/mapped_output.sql", #output file with mapped sql statements, to be used in transpiler
)

# Option 2: Map a single SQL statement with mapping file
# mapping_path = "mapping_master.xlsx"
# sql = "SELECT col FROM [schema].[table]"
# mapped = map_sql(sql, mapping_path)
# print(mapped)