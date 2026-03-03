from sql_mapper import map_sql, load_mapping

#sql = "SELECT LIFN2 FROM [raw_abdc_finance].[sapecc_wyt3]"

sql = """
WITH cte1 AS ( SELECT [BILL_NUM], [BILL_ITEM], FISCVARNT, CALMONTH, COUNT(*) AS duplicate_count FROM raw_abdc_operations.sapbw_billingtransactionlvl1 WHERE zzrefinv != '' AND bill_type IN ('zrk1', 'zrk2') AND ZPRICEOWN IN ('10', '20', '30', '40') AND salesorg IN ('1000', '2000', '1200') AND CAST(CREATED AS DATE) BETWEEN DATEADD(day, -180, GETDATE()) AND GETDATE() GROUP BY [BILL_NUM], [BILL_ITEM], FISCVARNT, CALMONTH ) SELECT 'BILL_NUM || BILL_ITEM || FISCVARNT || CALMONTH' AS PRIMARY_KEY_NAME, CAST(BILL_NUM AS VARCHAR) + ' || ' + CAST(BILL_ITEM AS VARCHAR) + ' || ' + CAST(FISCVARNT AS VARCHAR) + ' || ' + CAST(CALMONTH AS VARCHAR) AS PRIMARY_KEY_VALUE, 'BILL_NUM || BILL_ITEM || FISCVARNT || CALMONTH' AS ATTRIBUTE_NAME, CAST(BILL_NUM AS VARCHAR) + ' || ' + CAST(BILL_ITEM AS VARCHAR) + ' || ' + CAST(FISCVARNT AS VARCHAR) + ' || ' + CAST(CALMONTH AS VARCHAR) AS ATTRIBUTE_VALUE, 'duplicate_count' AS ADDITIONAL_COLUMN_NAME, duplicate_count AS ADDITIONAL_COLUMN_VALUE FROM cte1
"""

mapping_path = "conversion_file.xlsx"

mapped = map_sql(sql, mapping_path, filter_by_sql=True)
print(mapped)