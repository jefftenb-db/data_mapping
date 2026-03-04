SELECT * FROM adh_genpro_use2_prd.s_suppliers.partner_functions_hhd;
WITH cte1 AS ( SELECT `invoice_number`, `invc_itm_nbr`, fiscal_variant, doc_cal_month, COUNT(*) AS duplicate_count FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd WHERE ref_invc_nbr != '' AND invc_type IN ('zrk1', 'zrk2') AND sold_by IN ('10', '20', '30', '40') AND sales_org IN ('1000', '2000', '1200') AND CAST(created_date AS hist_cust_po_date) BETWEEN DATEADD(day, -180, current_timestamp()) AND current_timestamp() GROUP BY `invoice_number`, `invc_itm_nbr`, fiscal_variant, doc_cal_month ) SELECT 'BILL_NUM || BILL_ITEM || FISCVARNT || CALMONTH' AS PRIMARY_KEY_NAME, CAST(invoice_number AS STRING) || ' || ' || CAST(invc_itm_nbr AS STRING) || ' || ' || CAST(fiscal_variant AS STRING) || ' || ' || CAST(doc_cal_month AS STRING) AS PRIMARY_KEY_VALUE, 'BILL_NUM || BILL_ITEM || FISCVARNT || CALMONTH' AS ATTRIBUTE_NAME, CAST(invoice_number AS STRING) || ' || ' || CAST(invc_itm_nbr AS STRING) || ' || ' || CAST(fiscal_variant AS STRING) || ' || ' || CAST(doc_cal_month AS STRING) AS ATTRIBUTE_VALUE, 'duplicate_count' AS ADDITIONAL_COLUMN_NAME, duplicate_count AS ADDITIONAL_COLUMN_VALUE FROM cte1;

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, HEADER_EAR_DTTM_LOCAL, '49' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL);

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, CUST_ORD_NBR, '50' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL);

SELECT Distinct Descoped invoice_number&invc_itm_nbr, SOLD_BY,'18' AS MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') and  invc_type in ('zf2','zf2d','zf2f');

select distinct 
LTRIM(RTRIM(COALESCE(bp.object_row_id, '0')))AS EBP_ID,
TRIM(bp.partner_status_cd) AS partner_status_cd,
'EBP ID<>EBP Status Code<>Address Line 1<>Address Line 2<>Address Row ID<>Business Unit<>City<>Country Code<>First Name<>Last Name<>Local ERP ID<>Org Indv Flag<>Partner Name<>Postal Code<>State' as ADDITIONAL_COLUMN_NAME,
concat(bp.object_row_id,'<>',bp.partner_status_cd,'<>',A.address_line_1,'<>',A.address_line_2,'<>',A.object_row_id,'<>',bl.LOCAL_ERP_NAME,'<>',A.sold_to_city,'<>',A.COUNTRY_CODE,'<>',bp.partner_first_name,'<>',bp.partner_last_name,'<>',bl.LOCAL_ERP_ID,'<>',bp.org_indv_flg,'<>',bp.PARTNER_NAME,'<>',A.POSTAL_CODE,'<>',A.STATE_CODE_ORIG) as ADDITIONAL_COLUMN_VALUE,
'EBP ID Status Code does not meet Enterprise Standards.' AS INVALID_STATUS,
'1115' as MAPPING_ID
from adh_genpro_use2_prd.s_suppliers.business_partner_hhd bp  tablesample(10 percentage) 
join adh_genpro_use2_prd.s_reference_data.emdm_c_bp_local_hhd bl  on bl.ebp_id=bp.object_row_id
join adh_genpro_use2_prd.s_reference_data.c_bp_address_hhd A  on A.ebp_id=bp.object_row_id
where bp.HUB_STATE_IND =1 and bl.HUB_STATE_IND =1 and A.HUB_STATE_IND =1
and bp.org_indv_flg='O';

select distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, Manifest_EAR_DTTM_local, '34' as MAPPING_ID from adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  where (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL) and HEADER_EAR_DTTM_local &gt;= DATEADD(DAY, -90, CAST(current_timestamp() AS hist_cust_po_date)) and MANIFEST_EAR_DTTM_local &gt;= DATEADD(DAY, -90, CAST(current_timestamp() AS hist_cust_po_date));

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, HEADER_EAR_DTTM_LOCAL, '49' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL);

SELECT DISTINCT
cust_nbr,
cust_hrsa_nbr,
'Customer Account Name<>340B Contracted Pharmacy Flag<>Customer Group<>340B Partner Account # <>340B Partner Name' AS `ADDITIONAL_COLUMN_NAME`,
CONCAT(vendor_name_1,'<>',340b_contr_pharm_flg,'<>',cust_grp,'<>',340b_nbr,'<>',340b_ territory_name) AS `ADDITIONAL_COLUMN_VALUE`,
'1292' as Mapping_id
FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd

WHERE
((
    (340b_contr_pharm_flg = 'x' OR cust_grp IN ('07', '10'))
    AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (cust_hrsa_nbr IS NOT NULL and cust_hrsa_nbr != '')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (      retail_grp_1 = '20102'
                OR retail_grp_2 = '20102'
                OR retail_grp_3 = '20102'
                OR retail_grp_4 = '20102'
                OR retail_grp_5 = '20102'
			)
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND industry_cd IN ('48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (
                    `vendor_name_1` LIKE '%340B%'
                    OR `vendor_name_1` LIKE '% 340B %'
                    OR `vendor_name_1` LIKE '% 340B%'
                    OR `vendor_name_1` LIKE '%-340B%'
                )
                AND	
                (
                    `vendor_name_1` NOT LIKE '%-NON-340B%'
                    AND `vendor_name_1` NOT LIKE '% NON340B %'
                    AND `vendor_name_1` NOT LIKE '% NON 340B%'
                    AND `vendor_name_1` NOT LIKE '%-NON 340B%'
                    AND `vendor_name_1` NOT LIKE '% NON-340B%'
                )
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (340b_nbr IS NOT NULL or 340b_nbr != '')
		AND (340b_ territory_name IS NOT NULL or 340b_ territory_name != '')
));

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, acct_nbr, '53' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL);

select distinct CONCAT(aa.consolidated_attrib,',',aa.invoice_number,',',aa.prod_id,',',aa.acct_nbr) as Primary_Key,aa.consolidated_attrib as Ontime_CONS_ATTRIBUTE from  adh_genpro_use2_prd.g_product360.ontime_delivery_hhd as aa tablesample(10 percentage)  left join adh_genpro_use2_prd.s_transportation.emanifest_data_hhd as em   on aa.consolidated_attrib= em.consolidated_attrib left join adh_genpro_use2_prd.s_transportation.event_stop_header_hhd as ev  on aa.consolidated_attrib= ev.consolidated_attrib where (aa.consolidated_attrib IS NOT NULL OR aa.invoice_number IS NOT NULL OR aa.prod_id IS NOT NULL OR aa.acct_nbr IS NOT NULL);

select distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, Manifest_EAR_DTTM_local, '34' as MAPPING_ID from adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  where (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL) and HEADER_EAR_DTTM_local &gt;= DATEADD(DAY, -90, CAST(current_timestamp() AS hist_cust_po_date)) and MANIFEST_EAR_DTTM_local &gt;= DATEADD(DAY, -90, CAST(current_timestamp() AS hist_cust_po_date));

SELECT Distinct Descoped invoice_number&invc_itm_nbr,hist_ar_plant_key,'14' as MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') AND sold_by IN ('10', '20', '30', '40') and  invc_type in ('zf2','zf2d','zf2f') and hist_ar_plant_key is not null AND LTRIM(RTRIM(hist_ar_plant_key))!='';

SELECT 'DOC_NUMBER_ITEM_KEY' AS PRIMARY_KEY_NAME, SO.col_header as per mapping AS PRIMARY_KEY_VALUE, 'DOC_NUMBER_ITEM_KEY ' AS ATTRIBUTE_NAME, SO.col_header as per mapping AS ATTRIBUTE_VALUE, 'ORD_NBR' || '<>' || 'ORD_LINE_NBR' AS ADDITIONAL_COLUMN_NAME, CONCAT_WS('<>', SO.order_nbr, SO.order_line_nbr) AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.g_order360.order_transaction_hhd SO WHERE SO.MSG_CD IN ('ND', 'UM', 'NS', 'AM', 'NW', 'BO', 'TR') AND SO.ship_plant != '015' AND SO.order_nbr IS NOT NULL AND SO.SOLD_BY = '10' AND SO.created_date BETWEEN DATEADD(DAY, -180, current_timestamp()) AND current_timestamp();

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, prod_id AS ATTRIBUTE_VALUE, '39' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL) and prod_id IS NOT NULL and  ltrim(rtrim(prod_id))!='';

SELECT DISTINCT 
a.cust_nbr,
a.340b_contr_pharm_flg,
'Customer Account Name<>HRSA #<>340B Contracted Pharmacy Flag<>Customer Group<>340B Partner Account #' as `ADDITIONAL_COLUMN_NAME`,
CONCAT(a.vendor_name_1,'<>',a.cust_hrsa_nbr,'<>',a.340b_contr_pharm_flg,'<>',b.cust_grp,'<>',c.cust_account_num) AS `ADDITIONAL_COLUMN_VALUE`,
null as active_flg,
'1290' as Mapping_id
FROM adh_genpro_use2_prd.s_customers.general_customer_hhd a
LEFT JOIN adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd b on a.cust_nbr = b.cust_nbr
LEFT JOIN adh_genpro_use2_prd.s_customers.partner_function_hhd c on a.cust_nbr = c.cust_nbr AND c.owner_function = 'ZO'

WHERE
((
    (a.340b_contr_pharm_flg = 'X' OR b.cust_grp IN ('07', '10'))
    AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
    AND a.cust_acct_grp_cd in ('Z001', 'Z002') -- This filters results to Sold-To accounts only
)
OR
(
   b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
        AND a.cust_acct_grp_cd in ('Z001', 'Z002')
		AND (a.cust_hrsa_nbr IS NOT NULL and a.cust_hrsa_nbr != '')
)
OR
(
   b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
        AND a.cust_acct_grp_cd in ('Z001', 'Z002')
		AND (      a.retail_grp_1 = '20102'
                OR a.retail_grp_2 = '20102'
                OR a.retail_grp_3 = '20102'
                OR a.retail_grp_4 = '20102'
                OR a.retail_grp_5 = '20102'
			)
)
OR
(
   b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
		AND (
                    'NAME1' LIKE '%340B%'
                    OR 'NAME1' LIKE '% 340B %'
                    OR 'NAME1' LIKE '% 340B%'
                    OR 'NAME1' LIKE '%-340B%'
                )
                AND	
                (
                    'NAME1' NOT LIKE '%-NON-340B%'
                    AND 'NAME1' NOT LIKE '% NON340B %'
                    AND 'NAME1' NOT LIKE '% NON 340B%'
                    AND 'NAME1' NOT LIKE '%-NON 340B%'
                    AND 'NAME1' NOT LIKE '% NON-340B%'
                )
)
OR
(
  b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
        AND a.cust_acct_grp_cd in ('Z001', 'Z002')
        AND (c.owner_function = 'ZO')
)
);

SELECT   territory_id, end_date_c,'Name<>Account_Name__c<>Account__c<>Affiliation_Type__c<>IsDeleted' as `ADDITIONAL_COLUMN_NAME`,Concat_ws  ('<>',territory_name,account_name_c,acct__c,affiliation_type_c,is_del) as `ADDITIONAL_COLUMN_VALUE`,'1100' as Mapping_id FROM adh_genpro_use2_prd.s_customers.account_affiliation_hhd  WHERE affiliation_type_c = 'EBP';

SELECT DISTINCT cust_nbr AS PRIMARY_KEY_1,ncdp_nbr AS COLUMN_1, '1352' as MAPPING_ID FROM adh_genpro_use2_prd.s_customers.general_customer_hhd ;

SELECT distinct t1.cust_nbr as primary_key, t1.vendor_name_1 as DQ_Column1, t2.vendor_name_1 as DQ_Column2,'89' AS MAPPING_ID FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd t1 tablesample(10 percentage)  LEFT JOIN adh_genpro_use2_prd.s_customers.general_customer_hhd t2  ON t1.cust_nbr = t2.cust_nbr;

SELECT distinct t1.cust_nbr as primary_key, t1.postal_cd as DQ_Column1, t2.postal_cd as DQ_Column2,'143' AS MAPPING_ID FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd t1 tablesample(10 percentage)  LEFT JOIN adh_genpro_use2_prd.s_customers.general_customer_hhd t2  ON t1.cust_nbr = t2.cust_nbr;

SELECT distinct t1.cust_nbr as primary_key, t1.street as DQ_Column1, t2.street as DQ_Column2,'144' AS MAPPING_ID FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd t1 tablesample(10 percentage)  LEFT JOIN adh_genpro_use2_prd.s_customers.general_customer_hhd t2  ON t1.cust_nbr = t2.cust_nbr;

SELECT distinct t1.cust_nbr as primary_key, t1.sold_to_city as DQ_Column1, t2.sold_to_city as DQ_Column2,'172' AS MAPPING_ID FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd t1 tablesample(10 percentage)  LEFT JOIN adh_genpro_use2_prd.s_customers.general_customer_hhd t2  ON t1.cust_nbr = t2.cust_nbr;

SELECT Distinct Descoped invoice_number&invc_itm_nbr, invoice_number,'17' AS MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') and  invc_type in ('zf2','zf2d','zf2f');

select Distinct sb.Descoped invoice_number&invc_itm_nbr,sb.sold_to_party,so.cust_nbr, '1' AS MAPPING_ID from adh_genpro_use2_prd.g_order360.billing_transactions_hhd sb Left join adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd so on trim(sb.sold_to_party)=trim(so.cust_nbr) where sb.Invc_Date >= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) and sb.sold_by in ('10','20','30','40') and sb.sales_organization in ('1000','1200','2000') AND sold_to_party != 'ICC0033';

select distinct trim(sb.Descoped invoice_number&invc_itm_nbr) AS Descoped invoice_number&invc_itm_nbr,trim(sb.sold_to_party) AS Sold_To_billng,trim(sb.order_nbr) AS Ord_Nbr_billing,trim(sb.order_line_nbr) AS Ord_Line_Nbr_billing, trim(so.sold_to_party) AS Sold_To_order,trim(so.order_nbr) AS Ord_Nbr_order,trim(so.order_line_nbr) AS Ord_Line_Nbr_order, '3' as MAPPING_ID from adh_genpro_use2_prd.g_order360.billing_transactions_hhd sb tablesample(10 percentage)  left join adh_genpro_use2_prd.g_order360.order_transaction_hhd so tablesample(100 percentage)   on trim(sb.sold_to_party)=trim(so.sold_to_party) and trim(sb.order_nbr)=trim(so.order_nbr) and trim(sb.order_line_nbr)=trim(so.order_line_nbr) where sb.Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) and sb.sold_by in ('10','20','30','40') and sb.sales_organization in ('1000','1200','2000');

SELECT DISTINCT
cust_nbr,340b_nbr,
'Customer Account Name<>HRSA #<>340B Contracted Pharmacy Flag<>Customer Group' AS ADDITIONAL_COLUMN_NAME,
CONCAT(vendor_name_1,'<>',cust_hrsa_nbr,'<>',340b_contr_pharm_flg,'<>',cust_grp) AS ADDITIONAL_COLUMN_VALUE,

'1286' as Mapping_Id

FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd

WHERE
((
    (340b_contr_pharm_flg = 'x' OR cust_grp IN ('07', '10'))
    AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (cust_hrsa_nbr IS NOT NULL and cust_hrsa_nbr != '')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (      retail_grp_1 = '20102'
                OR retail_grp_2 = '20102'
                OR retail_grp_3 = '20102'
                OR retail_grp_4 = '20102'
                OR retail_grp_5 = '20102'
			)
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND industry_cd IN ('48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (
                    `vendor_name_1` LIKE '%340B%'
                    OR `vendor_name_1` LIKE '% 340B %'
                    OR `vendor_name_1` LIKE '% 340B%'
                    OR `vendor_name_1` LIKE '%-340B%'
                )
                AND	
                (
                    `vendor_name_1` NOT LIKE '%-NON-340B%'
                    AND `vendor_name_1` NOT LIKE '% NON340B %'
                    AND `vendor_name_1` NOT LIKE '% NON 340B%'
                    AND `vendor_name_1` NOT LIKE '%-NON 340B%'
                    AND `vendor_name_1` NOT LIKE '% NON-340B%'
                )
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (340b_nbr IS NOT NULL or 340b_nbr != '')
		AND (340b_ territory_name IS NOT NULL or 340b_ territory_name != '')
));

SELECT distinct t1.cust_nbr as primary_key, t1.sold_to_ctry as DQ_Column1, t2.ctry_key as DQ_Column2,'147' AS MAPPING_ID FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd t1 tablesample(10 percentage)  LEFT JOIN adh_genpro_use2_prd.s_customers.general_customer_hhd t2  ON t1.cust_nbr = t2.cust_nbr;

SELECT DISTINCT 
cust_nbr,
retail_grp_1,
'Customer Account Name<>HRSA #<>340B Contracted Pharmacy Flag<>Customer Group<>340B Partner Account #<>340B Partner Name' AS `ADDITIONAL_COLUMN_NAME`,
CONCAT(vendor_name_1,'<>',cust_hrsa_nbr,'<>',340b_contr_pharm_flg,'<>',cust_grp,'<>',340b_nbr,'<>',340b_ territory_name) AS `ADDITIONAL_COLUMN_VALUE`,
'1291' as Mapping_id
FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd

WHERE
((
    (340b_contr_pharm_flg = 'x' OR cust_grp IN ('07', '10'))
    AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (cust_hrsa_nbr IS NOT NULL and cust_hrsa_nbr != '')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (      retail_grp_1 = '20102'
                OR retail_grp_2 = '20102'
                OR retail_grp_3 = '20102'
                OR retail_grp_4 = '20102'
                OR retail_grp_5 = '20102'
			)
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND industry_cd IN ('48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (
                    `vendor_name_1` LIKE '%340B%'
                    OR `vendor_name_1` LIKE '% 340B %'
                    OR `vendor_name_1` LIKE '% 340B%'
                    OR `vendor_name_1` LIKE '%-340B%'
                )
                AND	
                (
                    `vendor_name_1` NOT LIKE '%-NON-340B%'
                    AND `vendor_name_1` NOT LIKE '% NON340B %'
                    AND `vendor_name_1` NOT LIKE '% NON 340B%'
                    AND `vendor_name_1` NOT LIKE '%-NON 340B%'
                    AND `vendor_name_1` NOT LIKE '% NON-340B%'
                )
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (340b_nbr IS NOT NULL or 340b_nbr != '')
		AND (340b_ territory_name IS NOT NULL or 340b_ territory_name != '')
));

SELECT distinct t1.cust_nbr as primary_key, t1.npi as DQ_Column1, t2.npi as DQ_Column2,'145' AS MAPPING_ID FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd t1 tablesample(10 percentage)  LEFT JOIN adh_genpro_use2_prd.s_customers.general_customer_hhd t2  ON t1.cust_nbr = t2.cust_nbr;

SELECT 'KEY_BILL_NUM_BILL_ITEM' AS PRIMARY_KEY_NAME, Descoped invoice_number&invc_itm_nbr AS PRIMARY_KEY_VALUE, 'INVC_DATE' AS ATTRIBUTE_NAME, INVC_DATE AS ATTRIBUTE_VALUE, 'ORD_DATE' AS ADDITIONAL_COLUMN_NAME, created_date AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd WHERE invc_type IN ('ZF2', 'ZF2F', 'ZF2D') AND SOLD_BY IN ('10', '20', '30', '40') AND sales_organization IN ('1000', '2000', '1200') AND created_date BETWEEN DATEADD(DAY, -180, current_timestamp()) AND current_timestamp();

select distinct
LTRIM(RTRIM(COALESCE(E.object_row_id, '0')))AS EBP_ID,
LTRIM(RTRIM(A.country)) AS country,
'EBP ID<>Address Line 1<>Address Line 2<>Address Row ID<>City<>Country Code<>County<>First Name<>Last Name<>Org Indv Flag<>Partner Name<>Postal Code<>State' as ADDITIONA_COLUMN_NAME,
concat(E.object_row_id,'<>',A.address_line_1,'<>',A.address_line_2,'<>',A.object_row_id,'<>',A.sold_to_city,'<>',A.COUNTRY_CODE,'<>',A.country,'<>',E.partner_first_name,'<>',E.partner_last_name,'<>',E.org_indv_flg,'<>',E.PARTNER_NAME,'<>',A.POSTAL_CODE,'<>',A.STATE_CODE_ORIG) as ADDITIONAL_COLUMN_VALUE,
'County code does not meet Enterprise Standards' AS INVALID_STATUS,
'1114' as MAPPING_ID
from adh_genpro_use2_prd.s_reference_data.c_bp_address_hhd A  JOIN adh_genpro_use2_prd.s_suppliers.business_partner_hhd E 
ON (A.EBP_ID=E.object_row_id AND E.HUB_STATE_IND!=-1 AND A.HUB_STATE_IND!=-1);

SELECT Distinct Descoped invoice_number&invc_itm_nbr, SOLD_BY,'18' AS MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') and  invc_type in ('zf2','zf2d','zf2f');

SELECT 'DOC_NUMBER_ITEM_KEY' AS PRIMARY_KEY_NAME, SO.col_header as per mapping AS PRIMARY_KEY_VALUE, 'DOC_NUMBER_ITEM_KEY ' AS ATTRIBUTE_NAME, SO.col_header as per mapping AS ATTRIBUTE_VALUE, 'ORD_NBR' || '<>' || 'ORD_LINE_NBR' AS ADDITIONAL_COLUMN_NAME, CONCAT_WS('<>', SO.order_nbr, SO.order_line_nbr) AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.g_order360.order_transaction_hhd SO WHERE SO.MSG_CD IN ('ND', 'UM', 'NS', 'AM', 'NW', 'BO', 'TR') AND SO.ship_plant != '015' AND SO.order_nbr IS NOT NULL AND SO.SOLD_BY = '10' AND SO.created_date BETWEEN DATEADD(DAY, -180, current_timestamp()) AND current_timestamp();

SELECT distinct t1.cust_nbr as primary_key, t1.region as DQ_Column1, t2.region as DQ_Column2,'148' AS MAPPING_ID FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd t1 tablesample(10 percentage)  LEFT JOIN adh_genpro_use2_prd.s_customers.general_customer_hhd t2  ON t1.cust_nbr = t2.cust_nbr;

SELECT distinct t1.cust_nbr as primary_key, t1.cont_nbr_1 as DQ_Column1, 'SAPECC_KNA1.TELF1' AS ADDITIONAL_COLUMN_NAME, t2.cont_nbr_1 AS ADDITIONAL_COLUMN_VALUE,'149' AS MAPPING_ID FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd t1 tablesample(10 percentage)  LEFT JOIN adh_genpro_use2_prd.s_customers.general_customer_hhd t2  ON t1.cust_nbr = t2.cust_nbr;

SELECT Distinct Descoped invoice_number&invc_itm_nbr, sales_organization,'19' AS MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') and  invc_type in ('zf2','zf2d','zf2f');

SELECT Distinct Descoped invoice_number&invc_itm_nbr, trailer_code,'6' as MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') AND sold_by IN ('10', '20', '30', '40') and  invc_type in ('zf2','zf2d','zf2f') and trailer_code is not null AND LTRIM(RTRIM(trailer_code))!='';

SELECT Distinct Descoped invoice_number&invc_itm_nbr, ship_plant,'7' as MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') AND sold_by IN ('10', '20', '30', '40') and  invc_type in ('zf2','zf2d','zf2f') and ship_plant is not null AND LTRIM(RTRIM(ship_plant))!='';

SELECT Distinct Descoped invoice_number&invc_itm_nbr, sales_organization,'19' AS MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') and  invc_type in ('zf2','zf2d','zf2f');

SELECT DISTINCT
a.cust_nbr,
c.owner_function,
'Customer Account Name<>HRSA #<>340B Contracted Pharmacy Flag<>Customer Group' AS ADDITIONAL_COLUMN_NAME,
CONCAT(a.vendor_name_1,'<>',a.cust_hrsa_nbr,'<>',a.340b_contr_pharm_flg,'<>',b.cust_grp) AS ADDITIONAL_COLUMN_VALUE,
'1289' as Mapping_id
FROM adh_genpro_use2_prd.s_customers.general_customer_hhd a
LEFT JOIN adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd b on a.cust_nbr = b.cust_nbr
LEFT JOIN adh_genpro_use2_prd.s_customers.partner_function_hhd c on a.cust_nbr = c.cust_nbr AND c.owner_function = 'ZO'

WHERE
((
    (a.340b_contr_pharm_flg = 'X' OR b.cust_grp IN ('07', '10'))
    AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
    AND a.cust_acct_grp_cd in ('Z001', 'Z002') -- This filters results to Sold-To accounts only
)
OR
(
   b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
        AND a.cust_acct_grp_cd in ('Z001', 'Z002')
		AND (a.cust_hrsa_nbr IS NOT NULL and a.cust_hrsa_nbr != '')
)
OR
(
   b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
        AND a.cust_acct_grp_cd in ('Z001', 'Z002')
		AND (      a.retail_grp_1 = '20102'
                OR a.retail_grp_2 = '20102'
                OR a.retail_grp_3 = '20102'
                OR a.retail_grp_4 = '20102'
                OR a.retail_grp_5 = '20102'
			)
)
OR
(
   b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
        AND a.cust_acct_grp_cd in ('Z001', 'Z002')
		AND (
                    `vendor_name_1` LIKE '%340B%'
                    OR `vendor_name_1` LIKE '% 340B %'
                    OR `vendor_name_1` LIKE '% 340B%'
                    OR `vendor_name_1` LIKE '%-340B%'
                )
                AND	
                (
                    `vendor_name_1` NOT LIKE '%-NON-340B%'
                    AND `vendor_name_1` NOT LIKE '% NON340B %'
                    AND `vendor_name_1` NOT LIKE '% NON 340B%'
                    AND `vendor_name_1` NOT LIKE '%-NON 340B%'
                    AND `vendor_name_1` NOT LIKE '% NON-340B%'
                )
)
OR
(
  b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
        AND a.cust_acct_grp_cd in ('Z001', 'Z002')
        AND (c.owner_function = 'ZO')
));

SELECT DISTINCT cust_nbr as `Primary Key value`
, cust_grp as `DQ Attribute Value`
, 'NAME1' AS ADDITIONAL_COLUMN_NAME
, vendor_name_1
 AS ADDITIONAL_COLUMN_VALUE
, '905' AS MAPPING_ID
FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd 
WHERE cust_corp_chain_nbr IN ('0500000153','0500005567','0500007520','0500046319')
AND cust_company_nbr NOT IN ('0400004511')
AND cust_central_ord_blk != 'Z1';

select distinct trim(sb.Descoped invoice_number&invc_itm_nbr) AS Descoped invoice_number&invc_itm_nbr,trim(sb.sold_to_party) AS Sold_To_billng,trim(sb.order_nbr) AS Ord_Nbr_billing,trim(sb.order_line_nbr) AS Ord_Line_Nbr_billing, trim(so.sold_to_party) AS Sold_To_order,trim(so.order_nbr) AS Ord_Nbr_order,trim(so.order_line_nbr) AS Ord_Line_Nbr_order, '3' as MAPPING_ID from adh_genpro_use2_prd.g_order360.billing_transactions_hhd sb tablesample(10 percentage)  left join adh_genpro_use2_prd.g_order360.order_transaction_hhd so tablesample(100 percentage)   on trim(sb.sold_to_party)=trim(so.sold_to_party) and trim(sb.order_nbr)=trim(so.order_nbr) and trim(sb.order_line_nbr)=trim(so.order_line_nbr) where sb.Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) and sb.sold_by in ('10','20','30','40') and sb.sales_organization in ('1000','1200','2000');

SELECT Distinct Descoped invoice_number&invc_itm_nbr, ship_plant,'7' as MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') AND sold_by IN ('10', '20', '30', '40') and  invc_type in ('zf2','zf2d','zf2f') and ship_plant is not null AND LTRIM(RTRIM(ship_plant))!='';

SELECT DISTINCT cust_nbr AS PRIMARY_KEY_1, hin_nbr AS COLUMN_1, '1304' as MAPPING_ID FROM adh_genpro_use2_prd.s_customers.general_customer_hhd;

SELECT DISTINCT cust_nbr AS PRIMARY_KEY_1, bus_prtnr_type AS COLUMN_1, '1386' as MAPPING_ID FROM adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd ;

SELECT DISTINCT cust_nbr AS PRIMARY_KEY_1, bus_prtnr_type AS COLUMN_1, '1386' as MAPPING_ID FROM adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd ;

SELECT DISTINCT cust_nbr AS PRIMARY_KEY_1,ncdp_nbr AS COLUMN_1, '1351' as MAPPING_ID FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd ;

select distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) AS PRIMARY_KEY_VALUE,Delta_from_manifest,'59' as MAPPING_ID from adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL);

SELECT Distinct Descoped invoice_number&invc_itm_nbr,invc_type,'12' as MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') AND sold_by IN ('10', '20', '30', '40') and  invc_type in ('zf2','zf2d','zf2f') and invc_type is not null AND LTRIM(RTRIM(invc_type))!='';

SELECT Distinct Descoped invoice_number&invc_itm_nbr,hist_ar_plant_key,'14' as MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') AND sold_by IN ('10', '20', '30', '40') and  invc_type in ('zf2','zf2d','zf2f') and hist_ar_plant_key is not null AND LTRIM(RTRIM(hist_ar_plant_key))!='';

SELECT Distinct Descoped invoice_number&invc_itm_nbr, invc_itm_nbr,'23' AS MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') and  invc_type in ('zf2','zf2d','zf2f');

SELECT Distinct Descoped invoice_number&invc_itm_nbr, invc_created_date,'24' AS MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') and  invc_type in ('zf2','zf2d','zf2f');

SELECT Distinct Descoped invoice_number&invc_itm_nbr,invc_type,'12' as MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') AND sold_by IN ('10', '20', '30', '40') and  invc_type in ('zf2','zf2d','zf2f') and invc_type is not null AND LTRIM(RTRIM(invc_type))!='';

SELECT distinct Descoped invoice_number&invc_itm_nbr,order_alt_serve_flag,ship_plant,hist_ar_plant_key,COALESCE(ship_plant,0) as SHIP_PLANT_null, COALESCE(hist_ar_plant_key,0) as HIST_AR_PLANT_CD_null, LENG,'31' AS MAPPING_ID FROM (SELECT Descoped invoice_number&invc_itm_nbr,ship_plant,hist_ar_plant_key,CASE WHEN LEN(order_alt_serve_flag)=0 THEN 'B' WHEN order_alt_serve_flag IS NULL THEN 'N' ELSE order_alt_serve_flag END  AS order_alt_serve_flag,case when LEN(order_alt_serve_flag) is null then 0 else LEN(order_alt_serve_flag) end as LENG FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  where adh_genpro_use2_prd.g_order360.billing_transactions_hhd. Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date))) A;

SELECT Distinct Descoped invoice_number&invc_itm_nbr, hist_cust_bus_chnl_nbr,'13' as MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') AND sold_by IN ('10', '20', '30', '40') and  invc_type in ('zf2','zf2d','zf2f') and hist_cust_bus_chnl_nbr is not null AND LTRIM(RTRIM(hist_cust_bus_chnl_nbr))!='';

select Distinct sb.Descoped invoice_number&invc_itm_nbr,sb.sold_to_party,so.cust_nbr, '1' AS MAPPING_ID from adh_genpro_use2_prd.g_order360.billing_transactions_hhd sb Left join adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd so on trim(sb.sold_to_party)=trim(so.cust_nbr) where sb.Invc_Date >= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) and sb.sold_by in ('10','20','30','40') and sb.sales_organization in ('1000','1200','2000') AND sold_to_party != 'ICC0033';

SELECT 'KEY_BILL_NUM_BILL_ITEM' AS PRIMARY_KEY_NAME, Descoped invoice_number&invc_itm_nbr AS PRIMARY_KEY_VALUE, 'INVC_DATE' AS ATTRIBUTE_NAME, INVC_DATE AS ATTRIBUTE_VALUE, 'ORD_DATE' AS ADDITIONAL_COLUMN_NAME, created_date AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd WHERE invc_type IN ('ZF2', 'ZF2F', 'ZF2D') AND SOLD_BY IN ('10', '20', '30', '40') AND sales_organization IN ('1000', '2000', '1200') AND created_date BETWEEN DATEADD(DAY, -180, current_timestamp()) AND current_timestamp();

SELECT Distinct Descoped invoice_number&invc_itm_nbr, sales_organization,'19' AS MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') and  invc_type in ('zf2','zf2d','zf2f');

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, cust_ship_to_zip, '40' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL) and cust_ship_to_zip IS NOT NULL and ltrim(rtrim(cust_ship_to_zip))!='';

select distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, Manifest_EAR_DTTM_local, '34' as MAPPING_ID from adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  where (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL) and HEADER_EAR_DTTM_local &gt;= DATEADD(DAY, -90, CAST(current_timestamp() AS hist_cust_po_date)) and MANIFEST_EAR_DTTM_local &gt;= DATEADD(DAY, -90, CAST(current_timestamp() AS hist_cust_po_date));

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, HEADER_EAR_DTTM_LOCAL, '49' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL);

SELECT Distinct Descoped invoice_number&invc_itm_nbr, order_line_nbr,'26' AS MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000')and  invc_type in ('zf2','zf2d','zf2f');

SELECT Distinct Descoped invoice_number&invc_itm_nbr, hist_cust_bus_chnl_nbr,'13' as MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') AND sold_by IN ('10', '20', '30', '40') and  invc_type in ('zf2','zf2d','zf2f') and hist_cust_bus_chnl_nbr is not null AND LTRIM(RTRIM(hist_cust_bus_chnl_nbr))!='';

SELECT Distinct Descoped invoice_number&invc_itm_nbr, order_type_code,'28' AS MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') and  invc_type in ('zf2','zf2d','zf2f');

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, invoice_number, '41' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL) and invoice_number IS NOT NULL and ltrim(rtrim(invoice_number))!='';

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, CUST_ORD_NBR, '50' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL);

select distinct CONCAT(aa.consolidated_attrib,',',aa.invoice_number,',',aa.prod_id,',',aa.acct_nbr) as Primary_Key,aa.consolidated_attrib as Ontime_CONS_ATTRIBUTE from  adh_genpro_use2_prd.g_product360.ontime_delivery_hhd as aa tablesample(10 percentage)  left join adh_genpro_use2_prd.s_transportation.emanifest_data_hhd as em   on aa.consolidated_attrib= em.consolidated_attrib left join adh_genpro_use2_prd.s_transportation.event_stop_header_hhd as ev  on aa.consolidated_attrib= ev.consolidated_attrib where (aa.consolidated_attrib IS NOT NULL OR aa.invoice_number IS NOT NULL OR aa.prod_id IS NOT NULL OR aa.acct_nbr IS NOT NULL);

SELECT DISTINCT cust_nbr as `Primary Key value`
, room_nbr as `DQ Attribute Value`
, 'Customer_Name<>5K Account BIGINT' as `ADDITIONAL_COLUMN_NAME`
, CONCAT(vendor_name_1,'<>',cust_corp_chain_nbr) as `ADDITIONAL_COLUMN_VALUE`
, '604' AS MAPPING_ID FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd  
WHERE cust_corp_chain_nbr in ('0500000153', '0500007520', '0500005567', '0500046319') AND cust_central_ord_blk != 'Z1' AND cust_company_nbr != '0400002260';

select Distinct p.Descoped invoice_number&invc_itm_nbr as Primary_Key, CONCAT(invoice_number,invc_itm_nbr) as DQ_Column,'29' AS MAPPING_ID from adh_genpro_use2_prd.g_order360.billing_transactions_hhd p tablesample(10 percentage)  LEFT join adh_genpro_use2_prd.g_order360.billing_transactions_hhd r  on Descoped invoice_number&invc_itm_nbr = CONCAT(invoice_number,invc_itm_nbr) where  Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) and invc_date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) and r.company_cd IN ('0061','0033') and p.invc_type IN ( 'zf2', 'zf2d', 'zf2f') and p.sales_organization IN ('1000','1200','2000') and p.Sold_By in ( '10', '20','30', '40');

SELECT 'KEY_BILL_NUM_BILL_ITEM' AS PRIMARY_KEY_NAME, Descoped invoice_number&invc_itm_nbr AS PRIMARY_KEY_VALUE, 'INVC_DATE' AS ATTRIBUTE_NAME, INVC_DATE AS ATTRIBUTE_VALUE, 'ORD_DATE' AS ADDITIONAL_COLUMN_NAME, created_date AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd WHERE invc_type IN ('ZF2', 'ZF2F', 'ZF2D') AND SOLD_BY IN ('10', '20', '30', '40') AND sales_organization IN ('1000', '2000', '1200') AND created_date BETWEEN DATEADD(DAY, -180, current_timestamp()) AND current_timestamp();

select distinct 
COALESCE(LTRIM(RTRIM(E.external_license_number)), '0') AS external_license_number,
COALESCE(LTRIM(RTRIM(E.validity_end_date)), '0') AS validity_end_date,
'Customer City<>Customer Name<>Customer BIGINT<>Customer Postal Code<>Customer State<>Customer Street<>EXT License BIGINT<>Legal Regulation<>Lecense Type<>License Valid End Date<>PLANT<>SAP License BIGINT' AS ADDITIONAL_COLUMN_NAME,
CONCAT(COALESCE(LTRIM(RTRIM(K.sold_to_city)), '0'),'<>',COALESCE(LTRIM(RTRIM(K.vendor_name_1)), '0'),'<>',COALESCE(LTRIM(RTRIM(K.cust_nbr)), '0'),'<>',COALESCE(LTRIM(RTRIM(K.postal_cd)), '0'),'<>',COALESCE(LTRIM(RTRIM(K.region)), '0'),'<>',COALESCE(LTRIM(RTRIM(K.street)), '0')
,'<>',COALESCE(LTRIM(RTRIM(E.external_license_number)), '0'),'<>',COALESCE(LTRIM(RTRIM(E.legal_regulation_legal_control)), '0'),'<>',COALESCE(LTRIM(RTRIM(E.license_type)), '0'),'<>',COALESCE(LTRIM(RTRIM(E.validity_end_date)), '0'),'<>',coalesce(nullif(K.transp_zone,''),'0'),'<>',COALESCE(LTRIM(RTRIM(E.gen_mat_nbr_prepack)), '0')) AS ADDITIONAL_COLUMN_VALUE,
CASE 
WHEN DATEDIFF(current_timestamp(), E.validity_end_date)<=30 AND DATEDIFF(current_timestamp(), E.validity_end_date)>0 THEN 'Organization License Will Expire in next 30 days or less ' 
WHEN E.validity_end_date < current_timestamp() THEN 'LICENSE EXPIRED' END AS `INVALID_STATUS`,
'1101' as Mapping_ID
from adh_genpro_use2_prd.s_customers.general_customer_hhd K 
 join adh_genpro_use2_prd.s_customers.partners_customers_license_hhd T 
on T.cust_nbr = K.cust_nbr
 join adh_genpro_use2_prd.s_customers.license_master_header_hhd E 
on E.gen_mat_nbr_prepack = T.gen_mat_nbr_prepack
where K.cust_acct_grp_cd in ('Z001', 'Z002' , 'Z013')
and K.vendor_name_1 not like ('*%')
and K.cust_central_ord_blk <> 'Z1'
and E.legal_control_stat = 'C';

SELECT Distinct cust_nbr AS PRIMARY_KEY_1,sold_to_bus_prtnr_type_cd AS COLUMN_1,'1385' as Mapping_id FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd ;

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, acct_nbr, '53' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL);

SELECT DISTINCT
cust_nbr,
cust_hrsa_nbr,
'Customer Account Name<>340B Contracted Pharmacy Flag<>Customer Group<>340B Partner Account # <>340B Partner Name' AS `ADDITIONAL_COLUMN_NAME`,
CONCAT(vendor_name_1,'<>',340b_contr_pharm_flg,'<>',cust_grp,'<>',340b_nbr,'<>',340b_ territory_name) AS `ADDITIONAL_COLUMN_VALUE`,
'1292' as Mapping_id
FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd

WHERE
((
    (340b_contr_pharm_flg = 'x' OR cust_grp IN ('07', '10'))
    AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (cust_hrsa_nbr IS NOT NULL and cust_hrsa_nbr != '')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (      retail_grp_1 = '20102'
                OR retail_grp_2 = '20102'
                OR retail_grp_3 = '20102'
                OR retail_grp_4 = '20102'
                OR retail_grp_5 = '20102'
			)
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND industry_cd IN ('48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (
                    `vendor_name_1` LIKE '%340B%'
                    OR `vendor_name_1` LIKE '% 340B %'
                    OR `vendor_name_1` LIKE '% 340B%'
                    OR `vendor_name_1` LIKE '%-340B%'
                )
                AND	
                (
                    `vendor_name_1` NOT LIKE '%-NON-340B%'
                    AND `vendor_name_1` NOT LIKE '% NON340B %'
                    AND `vendor_name_1` NOT LIKE '% NON 340B%'
                    AND `vendor_name_1` NOT LIKE '%-NON 340B%'
                    AND `vendor_name_1` NOT LIKE '% NON-340B%'
                )
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (340b_nbr IS NOT NULL or 340b_nbr != '')
		AND (340b_ territory_name IS NOT NULL or 340b_ territory_name != '')
));

SELECT Distinct Descoped invoice_number&invc_itm_nbr, order_type_code,'28' AS MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000') and  invc_type in ('zf2','zf2d','zf2f');

select distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key,time_zone,'57' as MAPPING_ID from adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL);

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, HEADER_EAR_DTTM_LOCAL, '49' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL);

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, prod_id AS ATTRIBUTE_VALUE, '39' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL) and prod_id IS NOT NULL and  ltrim(rtrim(prod_id))!='';

select distinct trim(sb.Descoped invoice_number&invc_itm_nbr) AS Descoped invoice_number&invc_itm_nbr,trim(sb.sold_to_party) AS Sold_To_billng,trim(sb.order_nbr) AS Ord_Nbr_billing,trim(sb.order_line_nbr) AS Ord_Line_Nbr_billing, trim(so.sold_to_party) AS Sold_To_order,trim(so.order_nbr) AS Ord_Nbr_order,trim(so.order_line_nbr) AS Ord_Line_Nbr_order, '3' as MAPPING_ID from adh_genpro_use2_prd.g_order360.billing_transactions_hhd sb tablesample(10 percentage)  left join adh_genpro_use2_prd.g_order360.order_transaction_hhd so tablesample(100 percentage)   on trim(sb.sold_to_party)=trim(so.sold_to_party) and trim(sb.order_nbr)=trim(so.order_nbr) and trim(sb.order_line_nbr)=trim(so.order_line_nbr) where sb.Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) and sb.sold_by in ('10','20','30','40') and sb.sales_organization in ('1000','1200','2000');

SELECT Distinct Descoped invoice_number&invc_itm_nbr, order_line_nbr,'26' AS MAPPING_ID FROM adh_genpro_use2_prd.g_order360.billing_transactions_hhd tablesample(10 percentage)  WHERE Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) AND sales_organization IN ('1000', '1200', '2000')and  invc_type in ('zf2','zf2d','zf2f');

select distinct CONCAT(aa.consolidated_attrib,',',aa.invoice_number,',',aa.prod_id,',',aa.acct_nbr) as Primary_Key,aa.consolidated_attrib as Ontime_CONS_ATTRIBUTE from  adh_genpro_use2_prd.g_product360.ontime_delivery_hhd as aa tablesample(10 percentage)  left join adh_genpro_use2_prd.s_transportation.emanifest_data_hhd as em   on aa.consolidated_attrib= em.consolidated_attrib left join adh_genpro_use2_prd.s_transportation.event_stop_header_hhd as ev  on aa.consolidated_attrib= ev.consolidated_attrib where (aa.consolidated_attrib IS NOT NULL OR aa.invoice_number IS NOT NULL OR aa.prod_id IS NOT NULL OR aa.acct_nbr IS NOT NULL);

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, CO_CODE, '52' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL);

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, acct_nbr, '53' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL);

select distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, Manifest_EAR_DTTM_local, '34' as MAPPING_ID from adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  where (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL) and HEADER_EAR_DTTM_local &gt;= DATEADD(DAY, -90, CAST(current_timestamp() AS hist_cust_po_date)) and MANIFEST_EAR_DTTM_local &gt;= DATEADD(DAY, -90, CAST(current_timestamp() AS hist_cust_po_date));

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, invoice_number, '41' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL) and invoice_number IS NOT NULL and ltrim(rtrim(invoice_number))!='';

SELECT distinct CONCAT(consolidated_attrib,',',invoice_number,',',prod_id,',',acct_nbr) as Primary_Key, cust_ship_to_zip, '40' AS MAPPING_ID FROM adh_genpro_use2_prd.g_product360.ontime_delivery_hhd tablesample(10 percentage)  WHERE (consolidated_attrib IS NOT NULL OR invoice_number IS NOT NULL OR prod_id IS NOT NULL OR acct_nbr IS NOT NULL) and cust_ship_to_zip IS NOT NULL and ltrim(rtrim(cust_ship_to_zip))!='';

SELECT DISTINCT
a.cust_nbr,a.retail_grp_1,
'Customer Account Name<>HRSA #<>340B Contracted Pharmacy Flag<>Customer Group<>340B Partner Account #' AS ADDITIONAL_COLUMN_NAME,
CONCAT(vendor_name_1,'<>',cust_hrsa_nbr,'<>',340b_contr_pharm_flg,'<>',cust_grp,'<>',cust_account_num) AS ADDITIONAL_COLUMN_VALUE,
'1284' as Mapping_Id

FROM adh_genpro_use2_prd.s_customers.general_customer_hhd a
LEFT JOIN adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd b on a.cust_nbr = b.cust_nbr
LEFT JOIN adh_genpro_use2_prd.s_customers.partner_function_hhd c on a.cust_nbr = c.cust_nbr AND c.owner_function = 'ZO'

WHERE
((
    (a.340b_contr_pharm_flg = 'X' OR b.cust_grp IN ('07', '10'))
    AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
    AND a.cust_acct_grp_cd in ('Z001', 'Z002') -- This filters results to Sold-To accounts only
)
OR
(
   b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
        AND a.cust_acct_grp_cd in ('Z001', 'Z002')
		AND (a.cust_hrsa_nbr IS NOT NULL and a.cust_hrsa_nbr != '')
)
OR
(
   b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
        AND a.cust_acct_grp_cd in ('Z001', 'Z002')
		AND (      a.retail_grp_1 = '20102'
                OR a.retail_grp_2 = '20102'
                OR a.retail_grp_3 = '20102'
                OR a.retail_grp_4 = '20102'
                OR a.retail_grp_5 = '20102'
			)
)
OR
(
   b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
		AND (
                    `vendor_name_1` LIKE '%340B%'
                    OR `vendor_name_1` LIKE '% 340B %'
                    OR `vendor_name_1` LIKE '% 340B%'
                    OR `vendor_name_1` LIKE '%-340B%'
                )
                AND	
                (
                    `vendor_name_1` NOT LIKE '%-NON-340B%'
                    AND `vendor_name_1` NOT LIKE '% NON340B %'
                    AND `vendor_name_1` NOT LIKE '% NON 340B%'
                    AND `vendor_name_1` NOT LIKE '%-NON 340B%'
                    AND `vendor_name_1` NOT LIKE '% NON-340B%'
                )
)
OR
(
  b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
        AND a.cust_acct_grp_cd in ('Z001', 'Z002')
        AND (c.owner_function = 'ZO')
));

SELECT DISTINCT
a.cust_nbr,
a.cust_hrsa_nbr,
'Customer Account Name<>340B Contracted Pharmacy Flag<>Customer Group<>340B Partner Account #' AS ADDITIONAL_COLUMN_NAME,
CONCAT(a.vendor_name_1,'<>',a.340b_contr_pharm_flg,'<>',b.cust_grp,'<>',c.cust_account_num) AS ADDITIONAL_COLUMN_VALUE,
'1285' as Mapping_Id
FROM adh_genpro_use2_prd.s_customers.general_customer_hhd a
LEFT JOIN adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd b on a.cust_nbr = b.cust_nbr
LEFT JOIN adh_genpro_use2_prd.s_customers.partner_function_hhd c on a.cust_nbr = c.cust_nbr AND c.owner_function = 'ZO'

WHERE
((
    (a.340b_contr_pharm_flg = 'X' OR b.cust_grp IN ('07', '10'))
    AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
    AND a.cust_acct_grp_cd in ('Z001', 'Z002') -- This filters results to Sold-To accounts only
)
OR
(
   b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
        AND a.cust_acct_grp_cd in ('Z001', 'Z002')
		AND (a.cust_hrsa_nbr IS NOT NULL and a.cust_hrsa_nbr != '')
)
OR
(
   b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
        AND a.cust_acct_grp_cd in ('Z001', 'Z002')
		AND (      a.retail_grp_1 = '20102'
                OR a.retail_grp_2 = '20102'
                OR a.retail_grp_3 = '20102'
                OR a.retail_grp_4 = '20102'
                OR a.retail_grp_5 = '20102'
			)
)
OR
(
   b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
		AND (
                    `vendor_name_1` LIKE '%340B%'
                    OR `vendor_name_1` LIKE '% 340B %'
                    OR `vendor_name_1` LIKE '% 340B%'
                    OR `vendor_name_1` LIKE '%-340B%'
                )
                AND	
                (
                    `vendor_name_1` NOT LIKE '%-NON-340B%'
                    AND `vendor_name_1` NOT LIKE '% NON340B %'
                    AND `vendor_name_1` NOT LIKE '% NON 340B%'
                    AND `vendor_name_1` NOT LIKE '%-NON 340B%'
                    AND `vendor_name_1` NOT LIKE '% NON-340B%'
                )
)
OR
(
  b.cust_grp IN ('02', '06', '08', '09', '12')
        AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '')
        AND a.cust_acct_grp_cd in ('Z001', 'Z002')
        AND (c.owner_function = 'ZO')
));

Select 'Customer Account BIGINT' as `PRIMARY_KEY_NAME`, cust_nbr as `Primary Key value`, 'Billing Plan BIGINT' as `ATTRIBUTE NAME`, bill_plan_nbr as `ATTRIBUTE VALUE`, 'Customer Account Name<>Order Block<>Customer 5K Account BIGINT' AS `ADDITIONAL_COLUMN_NAME`, concat(vendor_name_1,'<>',cust_central_ord_blk,'<>',cust_corp_chain_nbr) AS `ADDITIONAL_COLUMN_VALUE` from adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd where cust_corp_chain_nbr in ('0500000153', '0500046319') AND cust_company_nbr not in ('0400004511', '0400002260') AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = ' ');

select cust_nbr,drop_ship_mark_up,'Name1<>AUFSD<>CUSTOMER5X_NBR' AS ADDITIONAL_COLUMN_NAME , CONCAT(vendor_name_1,'<>',cust_central_ord_blk,'<>',cust_corp_chain_nbr)  AS ADDITIONAL_COLUMN_VALUE, null as active_flg, '1263' as mapping_id from adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd
where cust_corp_chain_nbr in ('0500000153', '0500007520', '0500046319') --Total population from these 5K accounts

AND cust_company_nbr not in ('0400004511', '0400002260') --Exclude these 4K accounts

AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '');

SELECT DISTINCT cust_nbr as `Primary Key value`
, room_nbr as `DQ Attribute Value`
, 'Customer_Name<>5K Account BIGINT' as `ADDITIONAL_COLUMN_NAME`
, CONCAT(vendor_name_1,'<>',cust_corp_chain_nbr) as `ADDITIONAL_COLUMN_VALUE`
, '604' AS MAPPING_ID FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd  
WHERE cust_corp_chain_nbr in ('0500000153', '0500007520', '0500005567', '0500046319') AND cust_central_ord_blk != 'Z1' AND cust_company_nbr != '0400002260';
WITH DSD as (SELECT DISTINCT room_nbr, cust_nbr, deployment_stat, vendor_name_1 FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd 
WHERE `cust_corp_chain_nbr` = '0500000153'
AND (deployment_stat IN ('01', '02', '03') OR deployment_stat IS NULL OR deployment_stat = '')
AND room_nbr IS NOT NULL
AND cust_central_ord_blk <> 'Z1'
AND cust_company_nbr NOT IN ('0400004168','0400004511')),

SMT AS (SELECT DISTINCT room_nbr, cust_nbr, deployment_stat, vendor_name_1 FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd 
WHERE `cust_corp_chain_nbr` = '0500007520'
AND (deployment_stat IN ('01', '02', '03') OR deployment_stat IS NULL OR deployment_stat = '')
AND room_nbr IS NOT NULL
AND cust_central_ord_blk <> 'Z1'
AND cust_company_nbr NOT IN ('0400004168','0400004511')),

B340 AS (SELECT DISTINCT room_nbr, cust_nbr, deployment_stat, vendor_name_1 FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd 
WHERE `cust_corp_chain_nbr` = '0500005567'
AND (deployment_stat IN ('01', '02', '03') OR deployment_stat IS NULL OR deployment_stat = '')
AND room_nbr IS NOT NULL
AND cust_central_ord_blk <> 'Z1'
AND cust_company_nbr NOT IN ('0400004168','0400004511')),

HPOP AS (SELECT DISTINCT room_nbr, cust_nbr, deployment_stat, vendor_name_1 FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd 
WHERE `cust_corp_chain_nbr` = '0500046319'
AND (deployment_stat IN ('01', '02', '03') OR deployment_stat IS NULL OR deployment_stat = '')
AND room_nbr IS NOT NULL
AND cust_central_ord_blk <> 'Z1'
AND cust_company_nbr NOT IN ('0400004168','0400004511'))


SELECT
  CONCAT('DSD~', t1.cust_nbr ,'<>','SMT~', t2.cust_nbr ,'<>','340B~', t3.cust_nbr ,'<>','HPOP~', t4.cust_nbr)  AS `PRIMARY_KEY_VALUE`,
  CONCAT('DSD~', t1.deployment_stat ,'<>', 'SMT~', t2.deployment_stat ,'<>', '340B~', t3.deployment_stat ,'<>', 'HPOP~', t4.deployment_stat)  AS `ATTRIBUTE_VALUE`,
  'ROOMNUMBER<>DSDNAME1<>SMTNAME1<>340BNAME1<>HPOPNAME1'  AS ADDITIONAL_COLUMN_NAME,
CONCAT(t1.room_nbr,'<>',t1.vendor_name_1,'<>',t2.vendor_name_1,'<>',t3.vendor_name_1,'<>',t4.vendor_name_1) AS ADDITIONAL_COLUMN_VALUE,
  '605' AS MAPPING_ID
FROM DSD t1
LEFT JOIN SMT t2 ON t1.room_nbr = t2.room_nbr 
LEFT JOIN B340 t3 ON t1.room_nbr = t3.room_nbr
LEFT JOIN HPOP t4 ON t1.room_nbr = t4.room_nbr;

select distinct kunnr_RG,ZZ_BU_MAIN_RG,
'Primary BU RG<>Primary BU ZF<>Primary BU ZG<>SAP Partner Function RG<>SAP Partner Function ZF<>SAP Partner Function ZG<>SAP Partner BIGINT RG<>SAP Partner BIGINT ZF<>SAP Partner BIGINT ZG<>SAP Sold To BIGINT'  AS ADDITIONAL_COLUMN_NAME,	
CONCAT(ZZ_BU_MAIN_RG,'<>',ZZ_BU_MAIN_ZF,'<>',ZZ_BU_MAIN_ZG,'<>',PARVW_RG,'<>',PARVW_ZF,'<>',PARVW_ZG,'<>',kunn2_RG,'<>',financial_resp_nbr,'<>',dist_rebate_nbr,'<>',kunnr_RG) AS ADDITIONAL_COLUMN_VALUE,
Case
when (Case
when L1.ZZ_BU_MAIN_RG IS NULL and L2.ZZ_BU_MAIN_ZF IS NULL then 'L1 and L2 Not Present'
when L2.ZZ_BU_MAIN_ZF IS NULL and L3.ZZ_BU_MAIN_ZG IS NULL then 'L2 and L3 Not Present'
when L1.ZZ_BU_MAIN_RG IS NULL and L3.ZZ_BU_MAIN_ZG IS NULL then 'L1 and L3 Not Present'
when L1.ZZ_BU_MAIN_RG IS NULL then'L1 Not Present'
when L2.ZZ_BU_MAIN_ZF IS NULL then'L2 Not Present'
when L3.ZZ_BU_MAIN_ZG IS NULL then'L3 Not Present' END) NOT IN('L1 Not Present','L2 Not Present','L3 Not Present','L1 and L2 Not Present','L2 and L3 Not Present','L1 and L3 Not Present' )and
(Case
when (L1.kunnr_RG IS NOT NULL and L2.kunnr_ZF IS NOT NULL and L3.kunnr_ZG IS NOT NULL) AND ((L1.ZZ_BU_MAIN_RG=L2.ZZ_BU_MAIN_ZF) and (L1.ZZ_BU_MAIN_RG=L3.ZZ_BU_MAIN_ZG) and (L2.ZZ_BU_MAIN_ZF=L3.ZZ_BU_MAIN_ZG))  then 'Y'
when (L1.kunnr_RG IS NOT NULL and L2.kunnr_ZF IS NOT NULL) AND ((L1.ZZ_BU_MAIN_RG=L2.ZZ_BU_MAIN_ZF)) then 'Y'
when (L1.kunnr_RG IS NOT NULL and L3.kunnr_ZG IS NOT NULL) AND ((L1.ZZ_BU_MAIN_RG=L3.ZZ_BU_MAIN_ZG)) then 'Y'
when (L2.kunnr_ZF IS NOT NULL and L3.kunnr_ZG IS NOT NULL) AND ((L2.ZZ_BU_MAIN_ZF=L3.ZZ_BU_MAIN_ZG)) then 'Y'
else 'N' END)='N' then 'SAP SoldToâ€™s relationships contain inconsistent Primary BU Main values.'
else '' END AS `INVALID_STATUS` 
,'1110' as MAPPING_ID from ((select knv.cust_nbr as kunnr_RG,knv.cust_account_num as kunn2_RG,knv.owner_function as PARVW_RG,kna.primary_business_unit as ZZ_BU_MAIN_RG  from adh_genpro_use2_prd.s_customers.partner_function_hhd knv
join adh_genpro_use2_prd.s_customers.general_customer_hhd kna on kna.cust_nbr=knv.cust_nbr and kna.cust_nbr=knv.cust_account_num
where kna.cust_central_ord_blk =''
and kna.deletion_flg_master_rec =''
and knv.owner_function in ('RG')  )L1 
full outer join(
select knv.cust_nbr as kunnr_ZF,knv.cust_account_num as financial_resp_nbr,knv.owner_function as PARVW_ZF,kna.primary_business_unit as ZZ_BU_MAIN_ZF from adh_genpro_use2_prd.s_customers.partner_function_hhd knv
join adh_genpro_use2_prd.s_customers.general_customer_hhd kna on kna.cust_nbr=knv.cust_nbr and kna.cust_nbr=knv.cust_account_num
where kna.cust_central_ord_blk =''
and kna.deletion_flg_master_rec =''
and knv.owner_function in ('ZF') ) L2 on L2.kunnr_ZF=L1.kunnr_RG
full outer join(
select knv.cust_nbr as kunnr_ZG,knv.cust_account_num as dist_rebate_nbr,knv.owner_function as PARVW_ZG,kna.primary_business_unit as ZZ_BU_MAIN_ZG from adh_genpro_use2_prd.s_customers.partner_function_hhd knv
join adh_genpro_use2_prd.s_customers.general_customer_hhd kna on kna.cust_nbr=knv.cust_nbr and kna.cust_nbr=knv.cust_account_num
where kna.cust_central_ord_blk =''
and kna.deletion_flg_master_rec =''
and knv.owner_function in ('ZG') ) L3 on L3.kunnr_ZG=L1.kunnr_RG);

select 
distinct KNVPkunnr,primary_business_unit,
'Primary BU RG<>Primary BU ZF<>Primary BU ZG<>SAP Partner Function RG<>SAP Partner Function ZF<>SAP Partner Function ZG<>SAP Partner BIGINT RG<>SAP Partner BIGINT ZF<>SAP Partner BIGINT ZG<>SAP Sold To BIGINT' AS ADDITIONAL_COLUMN_NAME,
CONCAT(primary_business_unit,'<>',ZZ_BU_MAIN_2,'<>',ZZ_BU_MAIN_3,'<>',knvpparvw,knvpparvw_2,'<>',knvpparvw_3,'<>',knvpKunn2,'<>',knvpKunn2_2,'<>',knvpKunn2_3,'<>',KNVPkunnr) AS ADDITIONAL_COLUMN_VALUE,
Case
when (Case
when L1.primary_business_unit IS NULL and L2.ZZ_BU_MAIN_2 IS NULL then 'L1 and L2 Not Present'
when L2.ZZ_BU_MAIN_2 IS NULL and L3.ZZ_BU_MAIN_3 IS NULL then 'L2 and L3 Not Present'
when L1.primary_business_unit IS NULL and L3.ZZ_BU_MAIN_3 IS NULL then 'L1 and L3 Not Present'
when L1.primary_business_unit IS NULL then'L1 Not Present'
when L2.ZZ_BU_MAIN_2 IS NULL then'L2 Not Present'
when L3.ZZ_BU_MAIN_3 IS NULL then'L3 Not Present' END) NOT IN('L1 Not Present','L2 Not Present','L3 Not Present','L1 and L2 Not Present','L2 and L3 Not Present','L1 and L3 Not Present' )and (Case
when (L1.KNVPkunnr IS NOT NULL and L2.KNVPkunnr_2 IS NOT NULL and L3.KNVPkunnr_3 IS NOT NULL) AND ((L1.primary_business_unit=L2.ZZ_BU_MAIN_2) and (L1.primary_business_unit=L3.ZZ_BU_MAIN_3) and (L2.ZZ_BU_MAIN_2=L3.ZZ_BU_MAIN_3))  then 'Y'
when (L1.KNVPkunnr IS NOT NULL and L2.KNVPkunnr_2 IS NOT NULL) AND ((L1.primary_business_unit=L2.ZZ_BU_MAIN_2)) then 'Y'
when (L1.KNVPkunnr IS NOT NULL and L3.KNVPkunnr_3 IS NOT NULL) AND ((L1.primary_business_unit=L3.ZZ_BU_MAIN_3)) then 'Y'
when (L2.KNVPkunnr_2 IS NOT NULL and L3.KNVPkunnr_3 IS NOT NULL) AND ((L2.ZZ_BU_MAIN_2=L3.ZZ_BU_MAIN_3)) then 'Y'
else 'N' END)='N' then 'SAP SoldToâ€™s relationships contain inconsistent Primary BU Main values.'
else '' END  AS `INVALID_STATUS`
,'1111' as MAPPING_ID from 
(select
      
COALESCE(TRIM(knvp.sold_to_nbr), '0') as KNVPkunnr,
COALESCE(TRIM(knvp.PRTNR_FUNC), '0')  as knvpparvw,
COALESCE(TRIM(knvp.bus_prtnr_cust_account_number), '0')  as knvpKunn2,
COALESCE(TRIM(KNA.primary_business_unit), '0') as primary_business_unit
from adh_genpro_use2_prd.s_pricing.prog_mat_details_a901_hhd.data_type as knvp
 join
adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd AS KNA
on knvp.sold_to_nbr=KNA.cust_nbr  and KNA.cust_nbr=knvp.bus_prtnr_cust_account_number
where KNA.cust_central_ord_blk='' 
and knvp.PRTNR_FUNC in('RG'))L1
full outer join
(select

knvp.sold_to_nbr as KNVPkunnr_2,
knvp.PRTNR_FUNC  as knvpparvw_2,
knvp.bus_prtnr_cust_account_number as knvpKunn2_2,
KNA.primary_business_unit as ZZ_BU_MAIN_2
from adh_genpro_use2_prd.s_pricing.prog_mat_details_a901_hhd.data_type as knvp
 join
adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd AS KNA
on knvp.sold_to_nbr=KNA.cust_nbr  and KNA.cust_nbr=knvp.bus_prtnr_cust_account_number
where KNA.cust_central_ord_blk='' 
and knvp.PRTNR_FUNC in('ZF'))L2 on L2.KNVPkunnr_2=L1.KNVPkunnr
full outer join
(select

knvp.sold_to_nbr as KNVPkunnr_3,
knvp.PRTNR_FUNC  as knvpparvw_3,
knvp.bus_prtnr_cust_account_number as knvpKunn2_3,
KNA.primary_business_unit as ZZ_BU_MAIN_3
from adh_genpro_use2_prd.s_pricing.prog_mat_details_a901_hhd.data_type as knvp
inner join
adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd AS KNA
on knvp.sold_to_nbr=KNA.cust_nbr  and KNA.cust_nbr=knvp.bus_prtnr_cust_account_number
where KNA.cust_central_ord_blk=''
and knvp.PRTNR_FUNC in('ZG'))L3 on L3.KNVPkunnr_3=L1.KNVPkunnr;

SELECT DISTINCT
cust_nbr,
340b_contr_pharm_flg,
'Customer Account Name<>HRSA #<>340B Contracted Pharmacy Flag<>Customer Group<>340B Partner Account # <>340B Partner Name' AS ADDITIONAL_COLUMN_NAME,
CONCAT(vendor_name_1,'<>',cust_hrsa_nbr,'<>',340b_contr_pharm_flg,'<>',cust_grp,'<>',340b_nbr,'<>',340b_ territory_name) AS ADDITIONAL_COLUMN_VALUE,
'1287' as Mapping_Id

FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd

WHERE
((
    (340b_contr_pharm_flg = 'x' OR cust_grp IN ('07', '10'))
    AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (cust_hrsa_nbr IS NOT NULL and cust_hrsa_nbr != '')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (      retail_grp_1 = '20102'
                OR retail_grp_2 = '20102'
                OR retail_grp_3 = '20102'
                OR retail_grp_4 = '20102'
                OR retail_grp_5 = '20102'
			)
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND industry_cd IN ('48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58')
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (
                    `vendor_name_1` LIKE '%340B%'
                    OR `vendor_name_1` LIKE '% 340B %'
                    OR `vendor_name_1` LIKE '% 340B%'
                    OR `vendor_name_1` LIKE '%-340B%'
                )
                AND	
                (
                    `vendor_name_1` NOT LIKE '%-NON-340B%'
                    AND `vendor_name_1` NOT LIKE '% NON340B %'
                    AND `vendor_name_1` NOT LIKE '% NON 340B%'
                    AND `vendor_name_1` NOT LIKE '%-NON 340B%'
                    AND `vendor_name_1` NOT LIKE '% NON-340B%'
                )
)
OR
(
   cust_grp IN ('02', '06', '08', '09', '12')
        AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '')
		AND (340b_nbr IS NOT NULL or 340b_nbr != '')
		AND (340b_ territory_name IS NOT NULL or 340b_ territory_name != '')
));

select cust_nbr, do_not_execute_repl_sub_splty_itm, 'Name1<>AUFSD<>CUSTOMER5X_NBR' AS ADDITIONAL_COLUMN_NAME, CONCAT(vendor_name_1,'<>',cust_central_ord_blk,'<>',cust_corp_chain_nbr) AS ADDITIONAL_COLUMN_VALUE, null as active_flg, '1262' as MAPPING_ID from adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd  where cust_corp_chain_nbr in ('0500000153', '0500007520', '0500046319') AND cust_company_nbr not in ('0400004511', '0400002260') AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '');

SELECT
    a.person_number,
    a.email_addr,
	'Customer Account BIGINT<>Customer Account Name' AS ADDITIONAL_COLUMN_NAME,
	concat(c.cust_nbr,'<>',c.vendor_name_1) AS ADDITIONAL_COLUMN_VALUE,
	'1509' as `Mapping_id`

FROM adh_genpro_use2_prd.s_customers.email_addr_hhd a
LEFT JOIN adh_genpro_use2_prd.s_customers.cust_master_cont_prtnr_hhd b ON a.person_number = b.person_nbr -- EAP sold_to_party cont_full_name Person table, holds cont_full_name info for people associated with a specific cust_nbr.
LEFT JOIN adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd c ON c.cust_nbr = b.cust_nbr --EAP sold_to_party Additional Address table, hold addt'l address data (i.e. email address for a customer)

WHERE
    ((c.KDGRP = '06') OR c.CUSTOMER5X_NBR in ('0500000153', '0500007520'))  
    --Filter by customer group '06â€™ as well as 2 specific WAG 5K parent_name accounts
    AND c.cust_bus_unit_nbr not in ('0300000293', '0300000143', '0300037766', '0300007204', '0300006400', '0300006401', '0300006382', '0300006399', '0300050573', '0300052827') --Exclude these 3K levels
	AND c.cust_central_ord_blk NOT IN ('Z1', 'Z6') -- No Central Order Block
	AND b.cont_person_dept_nbr = 'ZBCP';

select cust_nbr, sales_sgmnt, 
'Customer Account Name<>Order Block<>Customer 5K Account BIGINT' AS `ADDITIONAL_COLUMN_NAME`,
CONCAT(vendor_name_1,'<>',cust_central_ord_blk,'<>',cust_corp_chain_nbr) AS ADDITIONAL_COLUMN_VALUE,
'1266' as MAPPING_ID from adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd where cust_corp_chain_nbr in ('0500000153', '0500007520', '0500046319') AND cust_company_nbr not in ('0400004511', '0400002260') AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '');

select cust_nbr,invc_format_cd,
'Customer Account Name<>Order Block<>Customer 5K Account BIGINT' AS ADDITIONAL_COLUMN_NAME,
CONCAT(vendor_name_1,'<>',cust_central_ord_blk,'<>',cust_corp_chain_nbr) AS ADDITIONAL_COLUMN_VALUE,
null as active_flg,
'1261' as Mapping_Id
from adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd 
where cust_corp_chain_nbr in ('0500000153', '0500007520', '0500046319')
AND cust_company_nbr not in ('0400004511', '0400002260')
AND cust_bus_unit_nbr <> '0300050573'
AND (cust_central_ord_blk IS NULL OR cust_central_ord_blk = '');

select E.cust_nbr, E.sort_field,
'Customer Account Name<>Order Block<>Customer 5K Account BIGINT<>Created Date<>Created By' AS ADDITIONAL_COLUMN_NAME,
CONCAT(E.vendor_name_1,'<>',E.cust_central_ord_blk,'<>',E.cust_corp_chain_nbr,'<>',CAST(K.created_date AS STRING),'<>',K.created_by) AS ADDITIONAL_COLUMN_VALUE,
case
when (E.sort_field not in ('070008', '070011', '060008', '060006', '070008', '070011', '022849', '022869', '022760', '022762', '030353', '023661', '060010', '033768', '036754', '000568', '070020') OR TRIM(E.sort_field) = '' OR E.sort_field IS NULL) then 'N' 
else 'Y'
end as `INVALID_STATUS`,
'1267' as MAPPING_ID
from adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd E join adh_genpro_use2_prd.s_customers.general_customer_hhd K on E.cust_nbr = K.cust_nbr 
where E.cust_corp_chain_nbr in ('0500000153', '0500007520', '0500005567', '0500046319')
AND E.cust_company_nbr not in ('0400004511', '0400002260')
AND (E.cust_central_ord_blk IS NULL OR E.cust_central_ord_blk = '')
ORDER BY E.sort_field;

SELECT
    a.person_number,
    a.email_addr,
	'Customer Account BIGINT<>Customer Account Name' AS ADDITIONAL_COLUMN_NAME,
	concat(c.cust_nbr,'<>',c.vendor_name_1) AS ADDITIONAL_COLUMN_VALUE,
	'1509' as `Mapping_id`

FROM adh_genpro_use2_prd.s_customers.email_addr_hhd a
LEFT JOIN adh_genpro_use2_prd.s_customers.cust_master_cont_prtnr_hhd b ON a.person_number = b.person_nbr -- EAP sold_to_party cont_full_name Person table, holds cont_full_name info for people associated with a specific cust_nbr.
LEFT JOIN adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd c ON c.cust_nbr = b.cust_nbr --EAP sold_to_party Additional Address table, hold addt'l address data (i.e. email address for a customer)

WHERE
    ((c.KDGRP = '06') OR c.CUSTOMER5X_NBR in ('0500000153', '0500007520'))  
    --Filter by customer group '06â€™ as well as 2 specific WAG 5K parent_name accounts
    AND c.cust_bus_unit_nbr not in ('0300000293', '0300000143', '0300037766', '0300007204', '0300006400', '0300006401', '0300006382', '0300006399', '0300050573', '0300052827') --Exclude these 3K levels
	AND c.cust_central_ord_blk NOT IN ('Z1', 'Z6') -- No Central Order Block
	AND b.cont_person_dept_nbr = 'ZBCP';

SELECT DISTINCT
territory_id AS `Primary Key value`
, sap_acct_nbr as `DQ Attribute Value`
, '904' as `Mapping_id`
FROM adh_genpro_use2_prd.s_customers.account_hhd
WHERE (sap_acct_nbr IS NOT NULL OR TRIM(sap_acct_nbr)!='');

SELECT DISTINCT
territory_id AS `Primary Key value`
, sap_acct_nbr as `DQ Attribute Value`
, '904' as `Mapping_id`
FROM adh_genpro_use2_prd.s_customers.account_hhd
WHERE (sap_acct_nbr IS NOT NULL OR TRIM(sap_acct_nbr)!='');

select a.cust_nbr, b.maint_grp, 
'Customer Account Name<>Order Block<>Customer 5K Account BIGINT' AS `ADDITIONAL_COLUMN_NAME`,
CONCAT(a.vendor_name_1,'<>',a.cust_central_ord_blk,'<>',a.cust_corp_chain_nbr) AS ADDITIONAL_COLUMN_VALUE,
'1265' as MAPPING_ID from adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd a join adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd b on a.cust_nbr = b.cust_nbr where a.cust_corp_chain_nbr in ('0500000153', '0500007520', '0500046319') AND a.cust_company_nbr not in ('0400004511', '0400002260') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '');

select a.cust_nbr, b.maint_grp, 
'Customer Account Name<>Order Block<>Customer 5K Account BIGINT' AS `ADDITIONAL_COLUMN_NAME`,
CONCAT(a.vendor_name_1,'<>',a.cust_central_ord_blk,'<>',a.cust_corp_chain_nbr) AS ADDITIONAL_COLUMN_VALUE,
'1265' as MAPPING_ID from adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd a join adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd b on a.cust_nbr = b.cust_nbr where a.cust_corp_chain_nbr in ('0500000153', '0500007520', '0500046319') AND a.cust_company_nbr not in ('0400004511', '0400002260') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '');

SELECT distinct t1.cust_nbr as primary_key, t1.vendor_name_1 as DQ_Column1, t2.vendor_name_1 as DQ_Column2,'89' AS MAPPING_ID FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd t1 tablesample(10 percentage)  LEFT JOIN adh_genpro_use2_prd.s_customers.general_customer_hhd t2  ON t1.cust_nbr = t2.cust_nbr;

select a.cust_nbr, b.cust_prc_procedure, 
'Customer Account Name<>Order Block<>Customer 5K Account BIGINT' AS `ADDITIONAL_COLUMN_NAME`,	
CONCAT(a.vendor_name_1,'<>',a.cust_central_ord_blk,'<>',a.cust_corp_chain_nbr) AS ADDITIONAL_COLUMN_VALUE,
'1264' as MAPPING_ID from adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd a join adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd b on a.cust_nbr = b.cust_nbr where a.cust_corp_chain_nbr in ('0500000153', '0500007520', '0500046319') AND a.cust_company_nbr not in ('0400004511', '0400002260') AND a.cust_bus_unit_nbr != '0300050573' AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND NOT (a.cust_corp_chain_nbr = '0500007520' AND b.cust_prc_procedure = 'F');

select a.cust_nbr, b.cust_prc_procedure, 
'Customer Account Name<>Order Block<>Customer 5K Account BIGINT' AS `ADDITIONAL_COLUMN_NAME`,	
CONCAT(a.vendor_name_1,'<>',a.cust_central_ord_blk,'<>',a.cust_corp_chain_nbr) AS ADDITIONAL_COLUMN_VALUE,
'1264' as MAPPING_ID from adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd a join adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd b on a.cust_nbr = b.cust_nbr where a.cust_corp_chain_nbr in ('0500000153', '0500007520', '0500046319') AND a.cust_company_nbr not in ('0400004511', '0400002260') AND a.cust_bus_unit_nbr != '0300050573' AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND NOT (a.cust_corp_chain_nbr = '0500007520' AND b.cust_prc_procedure = 'F');

select E.cust_nbr,E.invc_conf_ctrl_cd,
'Customer Account Name<>Order Block<>Customer 5K Account BIGINT<>Created Date<>Created By' AS ADDITIONAL_COLUMN_NAME,
CONCAT(E.vendor_name_1,'<>',E.cust_central_ord_blk,'<>',E.cust_corp_chain_nbr,'<>',CAST(K.created_date AS STRING),'<>',K.created_by) AS ADDITIONAL_COLUMN_VALUE,
null as active_flg,
'1260' as Mapping_Id
from adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd E  join adh_genpro_use2_prd.s_customers.general_customer_hhd K  on E.cust_nbr = K.cust_nbr
where E.cust_corp_chain_nbr in ('0500000153', '0500007520', '0500046319')
AND E.cust_company_nbr not in ('0400004511', '0400002260')
AND E.cust_bus_unit_nbr <> '0300050573'
AND (E.cust_central_ord_blk IS NULL OR E.cust_central_ord_blk = '');

select Distinct p.Descoped invoice_number&invc_itm_nbr as Primary_Key, CONCAT(invoice_number,invc_itm_nbr) as DQ_Column,'29' AS MAPPING_ID from adh_genpro_use2_prd.g_order360.billing_transactions_hhd p tablesample(10 percentage)  LEFT join adh_genpro_use2_prd.g_order360.billing_transactions_hhd r  on Descoped invoice_number&invc_itm_nbr = CONCAT(invoice_number,invc_itm_nbr) where  Invc_Date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) and invc_date &gt;= DATEADD(DAY, -365, CAST(current_timestamp() AS hist_cust_po_date)) and r.company_cd IN ('0061','0033') and p.invc_type IN ( 'zf2', 'zf2d', 'zf2f') and p.sales_organization IN ('1000','1200','2000') and p.Sold_By in ( '10', '20','30', '40');

SELECT DISTINCT cust_nbr AS PRIMARY_KEY_1, hin_nbr AS COLUMN_1, '1303' as MAPPING_ID FROM adh_genpro_use2_prd.s_customers.customer_details_consolidated_hhd;

select 'PCNUM' AS PRIMARY_KEY_NAME, pricing_contract_number AS PRIMARY_KEY_VALUE, 'PCNUM' AS ATTRIBUTE_NAME, pricing_contract_number AS ATTRIBUTE_VALUE, 'VTDAT' AS ADDITIONAL_COLUMN_NAME, valid_to_date AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.s_contracts.contract_header_hhd where valid_to_date>=DATEADD(DAY, -180, current_timestamp());

SELECT 'Sold_To' || ' || ' || 'DATE' AS PRIMARY_KEY_NAME, sold_to_party || ' || ' || CAST(`hist_cust_po_date` AS STRING) AS PRIMARY_KEY_VALUE, 'DATE' AS ATTRIBUTE_NAME, `hist_cust_po_date` AS ATTRIBUTE_VALUE FROM adh_genpro_use2_prd.s_financials.enterprise_aging_hhd WHERE hist_cust_po_date>=DATEADD(DAY, -180, current_timestamp());

SELECT CAST(HashByteskey AS STRING) AS `PRIMARY_KEY_VALUE`, ACCURACY_STATUS_ADJ_QTY AS `ATTRIBUTE_VALUE`, 'ORD_QTY' || '<>' || 'WHSE_OUT_QTY' || '<>' || 'original_value' || '<>' || 'calculated_value' AS `ADDITIONAL_COLUMN_NAME`, CONCAT_WS('<>', total_ord_qty, WHSE_OUT_QTY,original_value,calculated_value) AS `ADDITIONAL_COLUMN_VALUE`, '1638' AS MAPPING_ID FROM ( SELECT HashByteskey,total_ord_qty,WHSE_OUT_QTY, (total_ord_qty - WHSE_OUT_QTY) AS calculated_value, ADJ_QTY AS original_value, CASE WHEN (total_ord_qty - WHSE_OUT_QTY) <> ADJ_QTY THEN 'INACCURATE' ELSE 'ACCURATE' END AS ACCURACY_STATUS_ADJ_QTY FROM data_type.data_type.data_type WHERE ORD_DT > DATEADD(cal_month, -6, current_timestamp()) ) AS results;

SELECT CAST(HashByteskey AS STRING) AS `PRIMARY_KEY_VALUE`, ACCURACY_STATUS_TOT_LN_OUTS AS `ATTRIBUTE_VALUE`, 'ORD_LN_CT' || '<>' || 'RAW_SERV_LN_CT' || '<>' || 'original_value' || '<>' || 'calculated_value' AS `ADDITIONAL_COLUMN_NAME`, CONCAT_WS('<>', ORD_LN_CT, RAW_SERV_LN_CT,ORIGINAL_VALUE,CALCULATED_VALUE) AS `ADDITIONAL_COLUMN_VALUE`, '1639' AS MAPPING_ID FROM ( SELECT HASHBYTESKEY,ORD_LN_CT,RAW_SERV_LN_CT, (ORD_LN_CT - RAW_SERV_LN_CT) AS calculated_value, TOT_LN_OUTS AS ORIGINAL_VALUE, CASE WHEN (ORD_LN_CT - RAW_SERV_LN_CT) <> TOT_LN_OUTS THEN 'INACCURATE' ELSE 'ACCURATE' END AS ACCURACY_STATUS_TOT_LN_OUTS FROM data_type.data_type.data_type WHERE ORD_DT > DATEADD(cal_month, -6, current_timestamp()) ) AS RESULTS;

SELECT CAST(HashByteskey AS STRING) AS `PRIMARY_KEY_VALUE`, ACCURACY_STATUS_ADJ_QTY_72_PCT AS `ATTRIBUTE_VALUE`, 'ADJ_QTY_72' || '<>' || 'ORD_QTY' || '<>' || 'original_value' || '<>' || 'calculated_value' AS `ADDITIONAL_COLUMN_NAME`, CONCAT_WS('<>', ADJ_QTY_72, total_ord_qty, original_value, calculated_value) AS `ADDITIONAL_COLUMN_VALUE`, '1640' AS MAPPING_ID FROM ( SELECT HashByteskey, ADJ_QTY_72, total_ord_qty, CAST(ROUND(ADJ_QTY_72 / NULLIF(total_ord_qty, 0) * 100, 3) AS DECIMAL(10,3)) AS calculated_value, ADJ_QTY_72_PCT AS original_value, CASE WHEN CAST(ROUND(ADJ_QTY_72 / NULLIF(total_ord_qty, 0) * 100, 3) AS DECIMAL(10, 3)) <> ADJ_QTY_72_PCT THEN 'INACCURATE' ELSE 'ACCURATE' END AS ACCURACY_STATUS_ADJ_QTY_72_PCT FROM data_type.data_type.data_type WHERE ORD_DT > DATEADD(cal_month, -6, current_timestamp()) ) AS results;

SELECT CAST(HashByteskey AS STRING) AS `PRIMARY_KEY_VALUE`, ACCURACY_STATUS_ADJ_QTY_72 AS `ATTRIBUTE_VALUE`, 'ADJ_QTY' || '<>' || 'WHSE_TMP_OUT_EXCP' || '<>' || 'WHSE_TMP_OUT_EXCP_50PCT' || '<>' || 'original_value' || '<>' || 'calculated_value' AS `ADDITIONAL_COLUMN_NAME`, CONCAT_WS('<>', ADJ_QTY, WHSE_TMP_OUT_EXCP,WHSE_TMP_OUT_EXCP_50PCT,original_value,calculated_value) AS `ADDITIONAL_COLUMN_VALUE`, '1641' as MAPPING_ID FROM ( SELECT HashByteskey,ADJ_QTY,WHSE_TMP_OUT_EXCP,WHSE_TMP_OUT_EXCP_50PCT,ADJ_QTY_72, (ADJ_QTY + WHSE_TMP_OUT_EXCP + WHSE_TMP_OUT_EXCP_50PCT) AS calculated_value, ADJ_QTY_72 AS original_value, CASE WHEN (ADJ_QTY + WHSE_TMP_OUT_EXCP + WHSE_TMP_OUT_EXCP_50PCT) = ADJ_QTY_72 THEN 'ACCURATE' ELSE 'INACCURATE' END AS ACCURACY_STATUS_ADJ_QTY_72 FROM data_type.data_type.data_type WHERE ORD_DT > DATEADD(cal_month, -6, current_timestamp()) ) AS results;

SELECT CAST(HashByteskey AS STRING) AS `PRIMARY_KEY_VALUE`, ACCURACY_STATUS_WHSE_TMP_OUT_EXCP_50PCT AS `ATTRIBUTE_VALUE`, 'WHSE_OUT_QTY' || '<>' || 'WHSE_TMP_OUT_EXCP' || '<>' || 'WHSE_TRU_TMP_OUT' || '<>' || 'original_value' || '<>' || 'calculated_value' AS `ADDITIONAL_COLUMN_NAME`, CONCAT_WS('<>', WHSE_OUT_QTY, WHSE_TMP_OUT_EXCP,WHSE_TRU_TMP_OUT,original_value,calculated_value) AS `ADDITIONAL_COLUMN_VALUE`, '1642' AS MAPPING_ID FROM ( SELECT HashByteskey,WHSE_OUT_QTY,WHSE_TMP_OUT_EXCP,WHSE_TRU_TMP_OUT,WHSE_TMP_OUT_EXCP_50PCT, (WHSE_OUT_QTY - WHSE_TMP_OUT_EXCP - WHSE_TRU_TMP_OUT) AS calculated_value, WHSE_TMP_OUT_EXCP_50PCT AS original_value, CASE WHEN (WHSE_OUT_QTY - WHSE_TMP_OUT_EXCP - WHSE_TRU_TMP_OUT) <> WHSE_TMP_OUT_EXCP_50PCT THEN 'INACCURATE' ELSE 'ACCURATE' END AS ACCURACY_STATUS_WHSE_TMP_OUT_EXCP_50PCT FROM data_type.data_type.data_type WHERE ORD_DT > DATEADD(cal_month, -6, current_timestamp()) ) AS results;

SELECT material_number || ' || ' || order_nbr AS `PRIMARY_KEY_VALUE`, service_ind AS `ATTRIBUTE_VALUE`, 'SalesOrderHeaderDate' AS `ADDITIONAL_COLUMN_NAME`, ord_created_date AS `ADDITIONAL_COLUMN_VALUE`, '1653' AS MAPPING_ID FROM adh_genpro_use2_prd.g_order360.service_level_history_hhd WHERE ord_created_date > DATEADD(day, -180, current_timestamp());

SELECT KNA1.cust_nbr,KNVV.sales_sgmnt,
'Company_Code<>Customer Create Date<>Customer Name<>Customer BIGINT<>Customer Sale Segement<>Sale Org' AS ADDITIONAL_COLUMN_NAME,
CONCAT(KNB1.co_cd,'<>',CAST(KNA1.created_date as STRING),'<>',KNA1.vendor_name_1,'<>',KNA1.cust_nbr,'<>',KNVV.sales_sgmnt,'<>',sales_organization) AS ADDITIONAL_COLUMN_VALUE,
'Customer Sales Segment is not populated.' AS INVALID_STATUS,
'1103' as Mapping_id  FROM adh_genpro_use2_prd.s_customers.general_customer_hhd  kna1 
LEFT JOIN adh_genpro_use2_prd.s_customers.cust_master_company_cd_hhd knb1  ON knb1.cust_nbr = kna1.cust_nbr 
LEFT JOIN adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd knvv   on knvv.cust_nbr = knb1.cust_nbr 
AND knvv.sales_organization in (1000,1200,2000)
WHERE kna1.cust_central_ord_blk =''
AND kna1.cust_acct_grp_cd in('Z001','Z002');

SELECT KNA1.cust_nbr,KNVV.sales_sgmnt,
'Company_Code<>Customer Create Date<>Customer Name<>Customer BIGINT<>Customer Sale Segement<>Sale Org' AS ADDITIONAL_COLUMN_NAME,
CONCAT(KNB1.co_cd,'<>',CAST(KNA1.created_date as STRING),'<>',KNA1.vendor_name_1,'<>',KNA1.cust_nbr,'<>',KNVV.sales_sgmnt,'<>',sales_organization) AS ADDITIONAL_COLUMN_VALUE,
'Customer Sales Segment is not populated.' AS INVALID_STATUS,
'1103' as Mapping_id  FROM adh_genpro_use2_prd.s_customers.general_customer_hhd  kna1 
LEFT JOIN adh_genpro_use2_prd.s_customers.cust_master_company_cd_hhd knb1  ON knb1.cust_nbr = kna1.cust_nbr 
LEFT JOIN adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd knvv   on knvv.cust_nbr = knb1.cust_nbr 
AND knvv.sales_organization in (1000,1200,2000)
WHERE kna1.cust_central_ord_blk =''
AND kna1.cust_acct_grp_cd in('Z001','Z002');

SELECT CAST(HashByteskey AS STRING) AS `PRIMARY_KEY_VALUE`, VALIDITY_STATUS_RAW_SERV_QTY AS `ATTRIBUTE_VALUE`, 'ORD_DT<>RAW_SERV_QTY<>ORD_QTY' AS `ADDITIONAL_COLUMN_NAME`, CAST(ORD_DT AS STRING) || '<>' || CAST(RAW_SERV_QTY AS STRING) || '<>' || CAST(total_ord_qty AS STRING) AS `ADDITIONAL_COLUMN_VALUE`, '1651' AS MAPPING_ID FROM ( SELECT HASHBYTESKEY, total_ord_qty, RAW_SERV_QTY, ORD_DT, CASE WHEN RAW_SERV_QTY > total_ord_qty THEN 'INVALID' ELSE 'VALID' END AS VALIDITY_STATUS_RAW_SERV_QTY FROM data_type.data_type.data_type WHERE ORD_DT > DATEADD(cal_month, -6, current_timestamp()) ) AS results;

SELECT CAST(HashByteskey AS STRING) AS `PRIMARY_KEY_VALUE`, VALIDITY_STATUS_RAW_SERV_LN_CT AS `ATTRIBUTE_VALUE`, 'ORD_DT"<>"RAW_SERV_LN_CT' AS `ADDITIONAL_COLUMN_NAME`, CAST(ORD_DT AS STRING)|| '<>' ||CAST(RAW_SERV_LN_CT AS STRING) AS `ADDITIONAL_COLUMN_VALUE`, '1652' AS MAPPING_ID FROM ( SELECT HASHBYTESKEY, RAW_SERV_LN_CT, ORD_DT, CASE WHEN RAW_SERV_LN_CT > ORD_LN_CT THEN 'INVALID' ELSE 'VALID' END AS VALIDITY_STATUS_RAW_SERV_LN_CT FROM data_type.data_type.data_type WHERE ORD_DT > DATEADD(cal_month, -6, current_timestamp()) ) AS results;

select distinct 
LTRIM(RTRIM(COALESCE(bp.object_row_id, '0')))AS EBP_ID,
TRIM(bp.partner_status_cd) AS partner_status_cd,
'EBP ID<>EBP Status Code<>Address Line 1<>Address Line 2<>Address Row ID<>Business Unit<>City<>Country Code<>First Name<>Last Name<>Local ERP ID<>Org Indv Flag<>Partner Name<>Postal Code<>State' as ADDITIONAL_COLUMN_NAME,
concat(bp.object_row_id,'<>',bp.partner_status_cd,'<>',A.address_line_1,'<>',A.address_line_2,'<>',A.object_row_id,'<>',bl.LOCAL_ERP_NAME,'<>',A.sold_to_city,'<>',A.COUNTRY_CODE,'<>',bp.partner_first_name,'<>',bp.partner_last_name,'<>',bl.LOCAL_ERP_ID,'<>',bp.org_indv_flg,'<>',bp.PARTNER_NAME,'<>',A.POSTAL_CODE,'<>',A.STATE_CODE_ORIG) as ADDITIONAL_COLUMN_VALUE,
'EBP ID Status Code does not meet Enterprise Standards.' AS INVALID_STATUS,
'1115' as MAPPING_ID
from adh_genpro_use2_prd.s_suppliers.business_partner_hhd bp  tablesample(10 percentage) 
join adh_genpro_use2_prd.s_reference_data.emdm_c_bp_local_hhd bl  on bl.ebp_id=bp.object_row_id
join adh_genpro_use2_prd.s_reference_data.c_bp_address_hhd A  on A.ebp_id=bp.object_row_id
where bp.HUB_STATE_IND =1 and bl.HUB_STATE_IND =1 and A.HUB_STATE_IND =1
and bp.org_indv_flg='O';

SELECT DISTINCT cust_nbr AS PRIMARY_KEY_1, hin_nbr AS COLUMN_1, '1304' as MAPPING_ID FROM adh_genpro_use2_prd.s_customers.general_customer_hhd;

select 'PCNUM' AS PRIMARY_KEY_NAME, pricing_contract_number AS PRIMARY_KEY_VALUE, 'VFDAT' AS ATTRIBUTE_NAME, valid_from_date AS ATTRIBUTE_VALUE, 'VTDAT' AS ADDITIONAL_COLUMN_NAME, valid_to_date AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.s_contracts.contract_header_hhd where valid_to_date>=DATEADD(DAY, -180, current_timestamp()) and valid_to_date is not null and valid_from_date IS NOT NULL and valid_from_date<>'' and valid_to_date<>'';

select 'PCNUM' AS PRIMARY_KEY_NAME, pricing_contract_number AS PRIMARY_KEY_VALUE, 'VTDAT' AS ATTRIBUTE_NAME, valid_to_date AS ATTRIBUTE_VALUE, 'VFDAT' AS ADDITIONAL_COLUMN_NAME, valid_from_date AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.s_contracts.contract_header_hhd where valid_to_date>=DATEADD(DAY, -180, current_timestamp());

SELECT 'Sold_To' || ' || ' || 'DATE' AS PRIMARY_KEY_NAME, sold_to_party || ' || ' || CAST(`hist_cust_po_date` AS STRING) AS PRIMARY_KEY_VALUE, '60D' AS ATTRIBUTE_NAME, `aging_60_days` AS ATTRIBUTE_VALUE, '`TOTAL_PD`'|| ' <> ' '`1-14D`' ' <> ' '`15-30D`' ' <> ' ||'`31-60D`' AS ADDITIONAL_COLUMN_NAME, CONCAT_WS(' <> ',`total_payments_due`,`aging_1_to_14_days`,`aging_15_to_30_days`,`aging_31_to_60_days`) AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.s_financials.enterprise_aging_hhd WHERE `hist_cust_po_date`>=DATEADD(DAY, -180, current_timestamp());
WITH CTE_COMBINED AS ( SELECT A.material_number, A.order_nbr, K.ord_line_nbr, K.sales_doc_nbr, A.service_ind, K.ZSERV_IND, CASE WHEN K.ZSERV_IND = 'Y' THEN 'VALID' ELSE 'INVALID' END AS VALIDITY_STATUS_SERVICELEVEL_IND FROM adh_genpro_use2_prd.g_order360.service_level_history_hhd A INNER JOIN data_type.data_type.data_type K ON A.material_number = K.ord_line_nbr AND A.order_nbr = K.sales_doc_nbr WHERE A.service_ind = 'Y' ) SELECT CAST(ord_line_nbr AS STRING) || ' || ' || CAST(sales_doc_nbr AS STRING) AS `PRIMARY_KEY_VALUE`, VALIDITY_STATUS_SERVICELEVEL_IND AS `ATTRIBUTE_VALUE`, 'ZSERV_IND' AS `ADDITIONAL_COLUMN_NAME`, ZSERV_IND AS `ADDITIONAL_COLUMN_VALUE`, '1666' as MAPPING_ID FROM CTE_COMBINED;
WITH cte_combined AS ( SELECT a.material_number, a.order_nbr, k.contr_src, k.ord_line_nbr, k.sales_doc_nbr, CASE WHEN k.contr_src = 'I' THEN 'VALID' ELSE 'INVALID' END AS VALIDITY_STATUS_SOURCE FROM adh_genpro_use2_prd.g_order360.service_level_history_hhd a INNER JOIN data_type.data_type.data_type k ON a.material_number = k.ord_line_nbr AND a.order_nbr = k.sales_doc_nbr ) SELECT CONCAT(ord_line_nbr, ' || ', sales_doc_nbr) AS `PRIMARY_KEY_VALUE`, VALIDITY_STATUS_SOURCE AS `ATTRIBUTE_VALUE`, 'SOURCE' AS `ADDITIONAL_COLUMN_NAME`, contr_src AS `ADDITIONAL_COLUMN_VALUE`, '1668' as MAPPING_ID FROM cte_combined;

SELECT CAST(HashByteskey AS STRING) AS `PRIMARY_KEY_VALUE`, SERV_IND AS `ATTRIBUTE_VALUE`, 'ORD_DT' AS `ADDITIONAL_COLUMN_NAME`, ORD_DT AS `ADDITIONAL_COLUMN_VALUE`, '1654' AS MAPPING_ID FROM data_type.data_type.data_type WHERE ORD_DT > DATEADD(DAY, -180, current_timestamp());

select 'PCNUM' AS PRIMARY_KEY_NAME, pricing_contract_number AS PRIMARY_KEY_VALUE, 'PCNUM' AS ATTRIBUTE_NAME, pricing_contract_number AS ATTRIBUTE_VALUE, 'VTDAT' AS ADDITIONAL_COLUMN_NAME, valid_to_date AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.s_contracts.contract_header_hhd where valid_to_date>=DATEADD(DAY, -180, current_timestamp());

SELECT 'Sold_To' || ' || ' || 'DATE' AS PRIMARY_KEY_NAME, sold_to_party || ' || ' || CAST(`hist_cust_po_date` AS STRING) AS PRIMARY_KEY_VALUE, 'TOTAL_PD' AS ATTRIBUTE_NAME, `total_payments_due` AS ATTRIBUTE_VALUE, '`1-30D`'|| ' <> ' ||'`31-60D`'|| ' <> ' ||'`60D`' AS ADDITIONAL_COLUMN_NAME, CONCAT_WS(' <> ',`aging_1_to_30_days`,`aging_31_to_60_days`,`aging_60_days`) AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.s_financials.enterprise_aging_hhd where `hist_cust_po_date`>=DATEADD(DAY, -180, current_timestamp());

SELECT CAST(HashByteskey AS STRING) AS `PRIMARY_KEY_VALUE`, ACCURACY_STATUS_MANF_LN_OUTS AS `ATTRIBUTE_VALUE`, 'TOT_LN_OUTS' || '<>' || 'WHSE_TMP_OUTS' || '<>' || 'original_value' || '<>' || 'calculated_value' AS `ADDITIONAL_COLUMN_NAME`, CONCAT_WS('<>', TOT_LN_OUTS, WHSE_TMP_OUTS,original_value,calculated_value) AS `ADDITIONAL_COLUMN_VALUE`, '1635' AS MAPPING_ID FROM ( SELECT HashByteskey,TOT_LN_OUTS,WHSE_TMP_OUTS,MANF_LN_OUTS, (TOT_LN_OUTS - WHSE_TMP_OUTS) AS calculated_value, MANF_LN_OUTS AS original_value, CASE WHEN (TOT_LN_OUTS - WHSE_TMP_OUTS) <> MANF_LN_OUTS THEN 'INACCURATE' ELSE 'ACCURATE' END AS ACCURACY_STATUS_MANF_LN_OUTS FROM data_type.data_type.data_type WHERE ORD_DT > DATEADD(cal_month, -6, current_timestamp()) ) AS results;

SELECT CAST(HashByteskey AS STRING) AS PRIMARY_KEY_VALUE, ACCURACY_STATUS_ADJSTD_LN_OUTS AS ATTRIBUTE_VALUE, 'ORD_LN_CT' || '<>' || 'WHSE_TMP_OUTS' || '<>' || 'original_value' || '<>' || 'calculated_value' AS ADDITIONAL_COLUMN_NAME, CONCAT_WS('<>', ORD_LN_CT, WHSE_TMP_OUTS, original_value, calculated_value) AS ADDITIONAL_COLUMN_VALUE, '1636' AS MAPPING_ID FROM ( SELECT HashByteskey, ORD_LN_CT, WHSE_TMP_OUTS, ADJSTD_LN_OUTS, (ORD_LN_CT - WHSE_TMP_OUTS) AS calculated_value, (ADJSTD_LN_OUTS) AS original_value, CASE WHEN (ORD_LN_CT - WHSE_TMP_OUTS) <> ADJSTD_LN_OUTS THEN 'INACCURATE' ELSE 'ACCURATE' END AS ACCURACY_STATUS_ADJSTD_LN_OUTS FROM data_type.data_type.data_type WHERE ORD_DT > DATEADD(cal_month, -6, current_timestamp()) ) AS results;

SELECT CAST(HashByteskey AS STRING) AS `PRIMARY_KEY_VALUE`, ACCURACY_STATUS_ADJSTD_LN_72INC_CT AS `ATTRIBUTE_VALUE`, 'ADJSTD_LN_OUTS' || '<>' || 'TRU_TMP_OUTS_EXCP' || '<>' || 'original_value' || '<>' || 'calculated_value' AS `ADDITIONAL_COLUMN_NAME`, CONCAT_WS('<>', ADJSTD_LN_OUTS, TRU_TMP_OUTS_EXCP,original_value,calculated_value) AS `ADDITIONAL_COLUMN_VALUE`, '1637' AS MAPPING_ID FROM ( SELECT HashByteskey,ADJSTD_LN_OUTS,TRU_TMP_OUTS_EXCP, (ADJSTD_LN_OUTS + TRU_TMP_OUTS_EXCP) AS calculated_value, ADJSTD_LN_72INC_CT AS original_value, CASE WHEN ((ADJSTD_LN_OUTS) + TRU_TMP_OUTS_EXCP) <> ADJSTD_LN_72INC_CT THEN 'INACCURATE' ELSE 'ACCURATE' END AS ACCURACY_STATUS_ADJSTD_LN_72INC_CT FROM data_type.data_type.data_type WHERE ORD_DT > DATEADD(cal_month, -6, current_timestamp()) ) AS results;
WITH CTE1 AS ( SELECT col_header as per mapping, delivery_item_number, delivery_number, COUNT(*) AS DUPLICATE_COUNT FROM adh_genpro_use2_prd.g_order360.abops_timetofill_hhd GROUP BY col_header as per mapping, delivery_item_number, delivery_number ) SELECT 'DOC_NUMBER_ITEM_KEY' || ' || ' || 'DELIV_ITEM' || ' || ' || 'DELIV_NUMB' AS `PRIMARY_KEY_NAME`, CAST(col_header as per mapping AS STRING) || ' || ' || CAST(delivery_item_number AS STRING) || ' || ' || CAST(delivery_number AS STRING) AS `PRIMARY_KEY_VALUE`, 'DOC_NUMBER_ITEM_KEY' || ' || ' || 'DELIV_ITEM' || ' || ' || 'DELIV_NUMB' AS `ATTRIBUTE_NAME`, CAST(col_header as per mapping AS STRING) || ' || ' || CAST(delivery_item_number AS STRING) || ' || ' || CAST(delivery_number AS STRING) AS `ATTRIBUTE_VALUE`, 'DUPLICATE_COUNT' AS `ADDITIONAL_COLUMN_NAME`, DUPLICATE_COUNT AS `ADDITIONAL_COLUMN_VALUE` FROM CTE1;

select 'PCNUM' AS PRIMARY_KEY_NAME, pricing_contract_number AS PRIMARY_KEY_VALUE, 'VFDAT' AS ATTRIBUTE_NAME, valid_from_date AS ATTRIBUTE_VALUE, 'VTDAT' AS ADDITIONAL_COLUMN_NAME, valid_to_date AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.s_contracts.contract_header_hhd where valid_to_date>=DATEADD(DAY, -180, current_timestamp()) and valid_to_date is not null and valid_from_date IS NOT NULL and valid_from_date<>'' and valid_to_date<>'';

SELECT 'Sold_To' || ' || ' || 'DATE' AS PRIMARY_KEY_NAME, sold_to_party || ' || ' || CAST(`hist_cust_po_date` AS STRING) AS PRIMARY_KEY_VALUE, 'OPEN_AR' AS ATTRIBUTE_NAME, open_accounts_receivable AS ATTRIBUTE_VALUE, 'CURRENT'|| '<>' ||'TOTAL_PD' AS ADDITIONAL_COLUMN_NAME, CONCAT_WS('<>',`Current_due_amt`,`total_payments_due`) AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.s_financials.enterprise_aging_hhd WHERE `hist_cust_po_date`>=DATEADD(DAY, -180, current_timestamp());

SELECT DISTINCT 'KUNNR' as PRIMARY_KEY_NAME, a.cust_nbr as PRIMARY_KEY_VALUE, 'Invoice Option Code 1' as ATTRIBUTE_NAME, b.invc_options_cd_1 as ATTRIBUTE_VALUE, 'Customer Account Name<>340B Contracted Pharmacy Flag<>Customer Group<>340B Partner Account #' AS ADDITIONAL_COLUMN_NAME, CONCAT(a.vendor_name_1,'<>',a.340b_contr_pharm_flg,'<>',b.cust_grp,'<>',c.cust_account_num) AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.s_customers.general_customer_hhd a LEFT JOIN adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd b on a.cust_nbr = b.cust_nbr LEFT JOIN adh_genpro_use2_prd.s_customers.partner_function_hhd c on a.cust_nbr = c.cust_nbr AND c.owner_function = 'ZO' WHERE (( (a.340b_contr_pharm_flg = 'X' OR b.cust_grp IN ('07', '10')) AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND a.cust_acct_grp_cd in ('Z001', 'Z002') ) OR ( b.cust_grp IN ('02', '06', '08', '09', '12') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND a.cust_acct_grp_cd in ('Z001', 'Z002') AND (a.cust_hrsa_nbr IS NOT NULL and a.cust_hrsa_nbr != '') ) OR ( b.cust_grp IN ('02', '06', '08', '09', '12') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND a.cust_acct_grp_cd in ('Z001', 'Z002') AND ( a.retail_grp_1 = '20102' OR a.retail_grp_2 = '20102' OR a.retail_grp_3 = '20102' OR a.retail_grp_4 = '20102' OR a.retail_grp_5 = '20102' OR a.retail_grp_6 = '20102' OR a.retail_grp_7 = '20102' OR a.retail_grp_8 = '20102' OR a.retail_grp_9 = '20102' OR a.retail_grp_10 = '20102' ) ) OR ( b.cust_grp IN ('02', '06', '08', '09', '12') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND ( `vendor_name_1` LIKE '%340B%' OR `vendor_name_1` LIKE '% 340B %' OR `vendor_name_1` LIKE '% 340B%' OR `vendor_name_1` LIKE '%-340B%' ) AND ( `vendor_name_1` NOT LIKE '%-NON-340B%' AND `vendor_name_1` NOT LIKE '% NON340B %' AND `vendor_name_1` NOT LIKE '% NON 340B%' AND `vendor_name_1` NOT LIKE '%-NON 340B%' AND `vendor_name_1` NOT LIKE '% NON-340B%' ) ) OR ( b.cust_grp IN ('02', '06', '08', '09', '12') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND a.cust_acct_grp_cd in ('Z001', 'Z002') AND (c.owner_function = 'ZO') ));

SELECT DISTINCT 'KUNNR' as PRIMARY_KEY_NAME, a.cust_nbr as PRIMARY_KEY_VALUE, 'Invoice Option Code 1' as ATTRIBUTE_NAME, b.invc_options_cd_1 as ATTRIBUTE_VALUE, 'Customer Account Name<>340B Contracted Pharmacy Flag<>Customer Group<>340B Partner Account #' AS ADDITIONAL_COLUMN_NAME, CONCAT(a.vendor_name_1,'<>',a.340b_contr_pharm_flg,'<>',b.cust_grp,'<>',c.cust_account_num) AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.s_customers.general_customer_hhd a LEFT JOIN adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd b on a.cust_nbr = b.cust_nbr LEFT JOIN adh_genpro_use2_prd.s_customers.partner_function_hhd c on a.cust_nbr = c.cust_nbr AND c.owner_function = 'ZO' WHERE (( (a.340b_contr_pharm_flg = 'X' OR b.cust_grp IN ('07', '10')) AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND a.cust_acct_grp_cd in ('Z001', 'Z002') ) OR ( b.cust_grp IN ('02', '06', '08', '09', '12') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND a.cust_acct_grp_cd in ('Z001', 'Z002') AND (a.cust_hrsa_nbr IS NOT NULL and a.cust_hrsa_nbr != '') ) OR ( b.cust_grp IN ('02', '06', '08', '09', '12') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND a.cust_acct_grp_cd in ('Z001', 'Z002') AND ( a.retail_grp_1 = '20102' OR a.retail_grp_2 = '20102' OR a.retail_grp_3 = '20102' OR a.retail_grp_4 = '20102' OR a.retail_grp_5 = '20102' OR a.retail_grp_6 = '20102' OR a.retail_grp_7 = '20102' OR a.retail_grp_8 = '20102' OR a.retail_grp_9 = '20102' OR a.retail_grp_10 = '20102' ) ) OR ( b.cust_grp IN ('02', '06', '08', '09', '12') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND ( `vendor_name_1` LIKE '%340B%' OR `vendor_name_1` LIKE '% 340B %' OR `vendor_name_1` LIKE '% 340B%' OR `vendor_name_1` LIKE '%-340B%' ) AND ( `vendor_name_1` NOT LIKE '%-NON-340B%' AND `vendor_name_1` NOT LIKE '% NON340B %' AND `vendor_name_1` NOT LIKE '% NON 340B%' AND `vendor_name_1` NOT LIKE '%-NON 340B%' AND `vendor_name_1` NOT LIKE '% NON-340B%' ) ) OR ( b.cust_grp IN ('02', '06', '08', '09', '12') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND a.cust_acct_grp_cd in ('Z001', 'Z002') AND (c.owner_function = 'ZO') ));

SELECT DISTINCT 'KUNNR' as PRIMARY_KEY_NAME, a.cust_nbr as PRIMARY_KEY_VALUE, 'Industry Code' as ATTRIBUTE_NAME, a.industry_cd as ATTRIBUTE_VALUE, 'Customer Account Name<>340B Contracted Pharmacy Flag<>Customer Group<>340B Partner Account #' AS ADDITIONAL_COLUMN_NAME, CONCAT(a.vendor_name_1,'<>',a.340b_contr_pharm_flg,'<>',b.cust_grp,'<>',c.cust_account_num) AS ADDITIONAL_COLUMN_VALUE FROM adh_genpro_use2_prd.s_customers.general_customer_hhd a LEFT JOIN adh_genpro_use2_prd.s_customers.cust_master_sales_data_hhd b on a.cust_nbr = b.cust_nbr LEFT JOIN adh_genpro_use2_prd.s_customers.partner_function_hhd c on a.cust_nbr = c.cust_nbr AND c.owner_function = 'ZO' WHERE (( (a.340b_contr_pharm_flg = 'X' OR b.cust_grp IN ('07', '10')) AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND a.cust_acct_grp_cd in ('Z001', 'Z002') ) OR ( b.cust_grp IN ('02', '06', '08', '09', '12') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND a.cust_acct_grp_cd in ('Z001', 'Z002') AND (a.cust_hrsa_nbr IS NOT NULL and a.cust_hrsa_nbr != '') ) OR ( b.cust_grp IN ('02', '06', '08', '09', '12') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND a.cust_acct_grp_cd in ('Z001', 'Z002') AND ( a.retail_grp_1 = '20102' OR a.retail_grp_2 = '20102' OR a.retail_grp_3 = '20102' OR a.retail_grp_4 = '20102' OR a.retail_grp_5 = '20102' OR a.retail_grp_6 = '20102' OR a.retail_grp_7 = '20102' OR a.retail_grp_8 = '20102' OR a.retail_grp_9 = '20102' OR a.retail_grp_10 = '20102' ) ) OR ( b.cust_grp IN ('02', '06', '08', '09', '12') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND ( `vendor_name_1` LIKE '%340B%' OR `vendor_name_1` LIKE '% 340B %' OR `vendor_name_1` LIKE '% 340B%' OR `vendor_name_1` LIKE '%-340B%' ) AND ( `vendor_name_1` NOT LIKE '%-NON-340B%' AND `vendor_name_1` NOT LIKE '% NON340B %' AND `vendor_name_1` NOT LIKE '% NON 340B%' AND `vendor_name_1` NOT LIKE '%-NON 340B%' AND `vendor_name_1` NOT LIKE '% NON-340B%' ) ) OR ( b.cust_grp IN ('02', '06', '08', '09', '12') AND (a.cust_central_ord_blk IS NULL OR a.cust_central_ord_blk = '') AND a.cust_acct_grp_cd in ('Z001', 'Z002') AND (c.owner_function = 'ZO') ));
