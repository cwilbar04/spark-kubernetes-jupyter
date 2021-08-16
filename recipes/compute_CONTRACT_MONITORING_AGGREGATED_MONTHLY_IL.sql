 REPLACE VIEW ${tbl:CONTRACT_MONITORING_AGGREGATED_MONTHLY_IL} AS (
 SELECT
     EXTRACT(MONTH FROM incurd_dt) as "Incurred Month"
     ,mbr_zip as "Member Zip Code"
     ,[CCSR Category Description]
     ,ACCT_NAME as "Account Name"
     ,provider_payee_name as "Provider Payee Name"
     ,inpat_outpat_cd as "Inpatiennt Outpatient Code"
     ,drg_cd as "DRG Code"
     ,[ICD-10-CM Codes Description]
     ,[HCPCS_CPT_Code_Desc]
     ,SUM([prov_allowed_amt]) as "Total Provider Allowed Amount"
     ,COUNT(distinct dw_clm_key) as "Total Claims"
     ,COUNT(distinct dw_mbr_key) as "Total Members"
     ,SUM(billd_amt) as "Total Billed Amount"