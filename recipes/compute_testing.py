# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.core.sql import SQLExecutor2
import python_recipe_utils as pru
from datetime import datetime, timedelta

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Recipe inputs - none

# Recipe outputs
CW_SANDBOX_CONTRACT_MONITORING_IL = dataiku.Dataset("CW_SANDBOX_CONTRACT_MONITORING_IL")
output_table_name = pru.get_qualified_table_name(CW_SANDBOX_CONTRACT_MONITORING_IL)
output_table_schema = output_table_name.split('.')[0]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Create a single dictionary that contains the recipe variables
client = dataiku.api_client()
proj_key = dataiku.default_project_key()
proj = client.get_project(proj_key)
recipe_vars = proj.get_variables()['standard']

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Create SQL Execution Engine
e = SQLExecutor2(connection=pru.get_connection_name(CW_SANDBOX_CONTRACT_MONITORING_IL))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# If re-create table = True then drop table and re-create using DDL
if recipe_vars['drop_and_recreate_table'] is True:

    drop_table_query = f'''
    CALL {output_table_schema}.DROP_TABLE_IF_EXISTS(
        '{output_table_name}')
    '''

    create_table_query = f'''
    CREATE MULTISET TABLE {output_table_name} ,
         NO BEFORE JOURNAL,
         NO AFTER JOURNAL,
         CHECKSUM = DEFAULT,
         DEFAULT MERGEBLOCKRATIO
         (
            --- Provider Info ---
            bill_pfin VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
            bill_pfin_10trimmed VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
            provider_payee_name VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC, -- why would billing provider be different than payee provider? Which one are we actually supposed to be monitoring here?
            primy_prcg_prov_spclty_cd CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,

            --- Member Info ---
            dw_mbr_key DECIMAL(18,0),
            gndr_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
            --age INTEGER, -- commented out for now as not needed. consider age grouping if needed in future
            mbr_zip VARCHAR(12) CHARACTER SET LATIN NOT CASESPECIFIC,
            mbr_city VARCHAR(28) CHARACTER SET LATIN NOT CASESPECIFIC,
            ACCT_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,

            --- Claim Info ---
            dw_clm_cntrl_key DECIMAL(18,0),
            claim_line_key VARCHAR(41) CHARACTER SET LATIN NOT CASESPECIFIC,
            dw_clm_key DECIMAL(18,0),
            li_num DECIMAL(4,0),
            incurd_dt DATE FORMAT 'YY/MM/DD',
            incurd_month DATE FORMAT 'YY/MM/DD',
            inpat_outpat_cd VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
            hcpcs_cpt_cd CHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
            HCPCS_CPT_Code_Desc VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            rvnu_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
            RevCD_Desc VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            HCPC_OR_REV_DESC VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            prim_diag VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            "ICD-10-CM Codes Description" VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            "CCSR Category Description" VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            DRG_CD CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
            tos_cat_cd CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
            tos_cat CHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            pos_cat_cd CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
            pos_cat CHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            --FINCL_ARNGMT_CD CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC, -- commented out for now. need to solve duplication issues before including if needed in future
            --FINCL_ARNGMT_CD_Desc VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            billd_amt DECIMAL(11,2),
            prov_alwd_amt DECIMAL(11,2),
            net_elig_amt DECIMAL(15,2),
            net_elig_rd_amt DECIMAL(15,2),
            net_pd_rd_amt DECIMAL(15,2),
            Net_Elig_or_RD_pd DECIMAL(15,2),
            LOS INTEGER
    )
    PRIMARY INDEX ( dw_clm_key ,li_num )
    '''
    e.query_to_df(query=create_table_query,
                 pre_queries=[drop_table_query])
    print(f'{output_table_name} dropped and re-created because drop_and_create_table varaible set to {recipe_vars["drop_and_recreate_table"]}')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Store the requested start and end dates as datetime values
# Need to add a check to make sure these are valid dates in the right format
# This allows us to split the query in to smaller parts if the spool can't handle all of it
requested_start_date = datetime.strptime(recipe_vars['start_date'],"%Y-%m-%d")
requested_end_date = datetime.strptime(recipe_vars['end_date'],"%Y-%m-%d")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Get current min/max date for claims in RADAR and those already moved to the Contract Monitoring table
min_max_radar_query = f'''
SELECT
    prov.prov_fincl_id as bill_pfin,
    min(ck.incurd_dt) as claim_radar_start_date,
    max(ck.incurd_dt) as claim_radar_end_date
FROM
RADAR_VIEWS.radardm_prod_claim AS ck
INNER JOIN RADAR_VIEWS.radardm_prod_claim_line AS clm_li
    ON ck.dw_clm_key = clm_li.dw_clm_key AND ck.source_schema_cd = clm_li.source_schema_cd
INNER JOIN RADAR_VIEWS.radardm_prod_provider prov
    on ck.billing_dw_prov_fincl_key = prov.dw_prov_fincl_key
WHERE
    ck.incurd_dt >= '{datetime.strftime(requested_start_date,"%Y-%m-%d")}'
    AND ck.incurd_dt <= '{datetime.strftime(requested_end_date,"%Y-%m-%d")}'
    AND ck.disp_cd = 'A'
    AND clm_li.disp_cd = 'A'
    AND ck.source_schema_cd = 'IL'
    AND ck.home_host_local_ind in ('HOME','LOCAL')
    AND prov.prov_fincl_id in {*recipe_vars['prov_finc_id_list_all'],}
GROUP BY 1
'''
min_max_contract_query = f'''
SELECT
    bill_pfin,
    min(incurd_dt) as claim_contract_start_date,
    max(incurd_dt) as claim_contract_end_date
FROM
{output_table_name}
GROUP BY 1
'''

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Store results of min/max query to pandas dataframe
min_max_radar = e.query_to_df(query=min_max_radar_query)
min_max_contract = e.query_to_df(query=min_max_contract_query)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Merge min/max dataframe to determine what needs to be loaded
all_dates = pd.merge(min_max_radar, min_max_contract, how='left')

# Mark all PFIN's where min/max dates match. These do not need to be loaded
all_dates['earliest_data_present'] = np.where(all_dates['claim_radar_start_date'] ==
                                              all_dates['claim_contract_start_date'], 1, 0)
all_dates['latest_data_present'] = np.where(all_dates['claim_radar_end_date'] ==
                                            all_dates['claim_contract_end_date'], 1, 0)

# Only need to load data if the start and end dates don't both match
missing_data = all_dates[~((all_dates['earliest_data_present'] == 1) & (all_dates['latest_data_present'] == 1))]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
to_load = pd.DataFrame(columns=['bill_pfin','start_date','end_date'])
first_of_current_month = datetime.today().replace(day=1)
for _,row in missing_data.iterrows():
    start_radar = datetime.strptime(row['claim_radar_start_date'], '%Y-%m-%dT%H:%M:%S.%fZ')
    end_radar = datetime.strptime(row['claim_radar_end_date'], '%Y-%m-%dT%H:%M:%S.%fZ')
    # If most recent radar data is this month then only load up until the end of the previous month
    # This will catch if RADAR is loading at the same time and make sure we don't lose data because of it
    # Need to investigate it RADAR data is re-created at the end of the month of if the "daily" data is
    # the source of truth and it is append only.
    if end_radar >= first_of_current_month:
        end_radar = first_of_current_month
    else:
        end_radar = end_radar + timedelta(days = 1)
    if (pd.isnull(row['claim_contract_start_date'])) & (pd.isnull(row['claim_contract_end_date'])):
        to_load = to_load.append({'bill_pfin':row['bill_pfin'],
                                  'start_date':start_radar,
                                  'end_date':end_radar},ignore_index=True)
        continue

    start_contract = datetime.strptime(row['claim_contract_start_date'], '%Y-%m-%dT%H:%M:%S.%fZ')
    if (start_radar < start_contract):
        to_load = to_load.append({'bill_pfin':row['bill_pfin'],
                                  'start_date':start_radar,
                                  'end_date':start_contract
                                 },ignore_index=True)
    end_contract = datetime.strptime(row['claim_contract_end_date'], '%Y-%m-%dT%H:%M:%S.%fZ')
    if (end_radar > end_contract):
        to_load = to_load.append({'bill_pfin':row['bill_pfin'],
                                  'start_date':end_contract + timedelta(days=1),
                                  'end_date':end_radar
                                 },ignore_index=True)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#to_load

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
to_load['bill_pfin'] = to_load['bill_pfin'].astype(str).str.zfill(13)
start_time = datetime.now()
print(f'Load process started at {start_time}')
for _,row in to_load.iterrows():
    pfin = row['bill_pfin']
    start_date = row['start_date']
    end_date = row['end_date']
    actual_end_date = row['end_date']
    delta = end_date-start_date
    while start_date < actual_end_date:
        try:
            insert_query = f'''
            INSERT INTO {output_table_name}
    WITH all_claims_with_rd AS (
        SELECT
        	b.*
        	, tos_cat_desc.CODE_TXT as tos_cat
            , pos_cat_desc.CODE_TXT as pos_cat
            , CASE
                WHEN claim_type = 'institutional_outpatient_visit' THEN
                    sum(net_elig_proportion) OVER
                        (PARTITION BY dw_clm_key, hcpcs_cpt_cd, svc_from_dt, svc_to_dt, rvnu_cd )
                WHEN claim_type = 'institutional_inpatient_admission' THEN
                    sum(net_elig_proportion) OVER
                        (PARTITION BY dw_clm_key)
                ELSE 1
             END as proportion_total
            , CASE
                WHEN claim_type = 'institutional_outpatient_visit' THEN
                    ROW_NUMBER() OVER
                        (PARTITION BY dw_clm_key, hcpcs_cpt_cd, svc_from_dt
                            , svc_to_dt, rvnu_cd
                         ORDER BY li_num)
                WHEN claim_type in ('institutional_inpatient_admission') THEN
                    ROW_NUMBER() OVER
                        (PARTITION BY dw_clm_key
                         ORDER BY li_num)
                ELSE 0
             END as line_number_order
            , CASE
                WHEN line_number_order = 1 THEN net_elig_proportion + (1-proportion_total)
                ELSE net_elig_proportion
            END as actual_proportion
            , net_pd_rd_amt_psi * actual_proportion as net_pd_rd_amt
            , net_elig_rd_amt_psi * actual_proportion as net_elig_rd_amt
        FROM
            (
            SELECT
                a.*
                , ROUND(
                    CASE
                        WHEN claim_type = 'institutional_outpatient_visit' THEN
                            ZEROIFNULL(
                                ZEROIFNULL(net_elig_amt)
                                    /
                                NULLIFZERO(sum(net_elig_amt) OVER (
                                            PARTITION BY
                                            dw_clm_key
                                            , hcpcs_cpt_cd
                                            , svc_from_dt
                                            , svc_to_dt
                                            , rvnu_cd
                                ))
                                )
                        WHEN claim_type = 'institutional_inpatient_admission' THEN
                            ZEROIFNULL(
                                ZEROIFNULL(net_elig_amt)
                                    /
                                NULLIFZERO((sum(net_elig_amt) OVER (
                                            PARTITION BY
                                            dw_clm_key
                                )))
                                )
                        ELSE 1
                    END
                   ,2) as net_elig_proportion
            FROM

                (
                SELECT
                    DISTINCT
                    prov.prov_fincl_id
                    , ck.provider_payee_name
                    , prov.primy_prcg_prov_spclty_cd
            --- Member Info ---
                    , ck.dw_mbr_key
                    , ck.dw_acct_key
             --- Claim Info ---
                    , ck.dw_clm_cntrl_key
                    , ck.dw_clm_key
                    , clm_li.Li_num
                    , concat(to_char(clm_li.dw_clm_key),'-',to_char(clm_li.Li_num)) as "claim_line_key"
                    , ck.incurd_dt
                    , clm_li.inpat_outpat_cd
                    , clm_li.svc_from_dt
                    , clm_li.svc_to_dt
                    , clm_li.pd_dt
                    , clm_li.rvnu_cd
                    , clm_li.HCPCS_CPT_Cd
                    , clm_li.primy_diag_cd
                    , CASE
                        WHEN clm_li.clm_filing_cd = '01' THEN
                            CASE
                                WHEN vi.dw_clm_key is not NULL THEN 'institutional_outpatient_visit'
                                WHEN adm.dw_clm_key is not NULL THEN 'institutional_inpatient_admission'
                                WHEN ipn.dw_clm_key is not NULL THEN 'institutional_inpatient_other'
                                ELSE 'other_institutional'
                            END
                        WHEN clm_li.clm_filing_cd = '02' THEN 'professional'
                        ELSE 'other_not_01_or_02_why'
                    END as claim_type
                    -- Outpatient visits in DSL can be split in to multiple lines with the same values for all
                    -- of the join columns if the GRVU can only be calculated for a portion of a visit that would
                    -- otherwise normally be grouped together. In this case, the RD amounts are split across these
                    -- two lines so we need to sum across this partition and select distinct values only
                    , COALESCE(sum(vi.net_elig_rd_amt) OVER (PARTITION BY clm_li.dw_clm_key, clm_li.li_num)
                            , adm.net_elig_rd_amt
                            , ipn.net_elig_rd_amt) as net_elig_rd_amt_psi
                    , COALESCE(sum(vi.net_pd_rd_amt) OVER (PARTITION BY clm_li.dw_clm_key, clm_li.li_num)
		                    , adm.net_pd_rd_amt
		                    , ipn.net_pd_rd_amt) as net_pd_rd_amt_psi
                    , clm_li.billd_amt
                    , clm_li.prov_alwd_amt
                    , clm_li.net_elig_amt
                    , CASE 
                    	WHEN vi.dw_visit_key is not NULL THEN 'Outpatient'
                    	WHEN adm.dw_adm_key is not NULL OR ipn_cat.dw_clm_key is not NULL THEN 'Inpatient'
                    	WHEN proc.dw_clm_key is not NULL THEN 'Professional'
                    	ELSE 'Not in DSL'
                    END as "DSL_CLM_TYP"
                    , COALESCE (vi_cat.tos_cat, adm_cat.tos_cat, ipn_cat.tos_cat, proc_cat.tos_cat) as tos_cat_cd
--                    , COALESCE (vi_tos_cat_desc.code_txt, adm_tos_cat_desc.code_txt, ipn_tos_cat_desc.code_txt, proc_tos_cat_desc.code_txt) as tos_cat
                    , COALESCE (vi_cat.pos_cat, adm_cat.pos_cat, ipn_cat.pos_cat, proc_cat.pos_cat) as pos_cat_cd
--                    , COALESCE (vi_pos_cat_desc.code_txt, adm_pos_cat_desc.code_txt, ipn_pos_cat_desc.code_txt, proc_pos_cat_desc.code_txt) as pos_cat
                FROM "RADAR_VIEWS"."radardm_prod_claim" AS ck
                INNER JOIN RADAR_VIEWS.radardm_prod_claim_line AS clm_li ON ck.dw_clm_key = clm_li.dw_clm_key
                    AND ck.source_schema_cd = clm_li.source_schema_cd
                INNER JOIN RADAR_VIEWS.radardm_prod_provider prov on ck.billing_dw_prov_fincl_key = prov.dw_prov_fincl_key
                -- Insitutional Outpatient Visits
                LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_visit vi ON
                    vi.dw_clm_key = ck.dw_clm_key
                    and clm_li.hcpcs_cpt_cd = vi.hcpcs_cpt_cd
                    and clm_li.svc_from_dt = vi.svc_from_dt
                    and clm_li.svc_to_dt = vi.svc_to_dt
                    and clm_li.rvnu_cd = vi.rvnu_cd
                    and vi.pd_thru_dt = DATE '9999-12-31'
                LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_visit_cat vi_cat ON
                	vi_cat.dw_visit_key = vi.dw_visit_key
--                LEFT JOIN ENTPR_BP_ADS_VIEWS.dsl_code_table vi_tos_cat_desc ON
--                	vi_cat.tos_cat = vi_tos_cat_desc.CODE_CD 
--                	AND vi_tos_cat_desc.COLUMN_NAME = 'tos_cat'
--                	AND vi_tos_cat_desc.CODE_CLM_TYP ='Outpatient'
--                LEFT JOIN ENTPR_BP_ADS_VIEWS.dsl_code_table vi_pos_cat_desc ON
--                	vi_cat.pos_cat = vi_pos_cat_desc.CODE_CD 
--                	AND vi_pos_cat_desc.COLUMN_NAME = 'pos_cat'
--                	AND vi_pos_cat_desc.CODE_CLM_TYP ='Outpatient'
                	
                -- Institutional Inpatient Admissions	
                LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_adm adm ON
                    adm.dw_clm_key = ck.dw_clm_key
                    and adm.pd_thru_dt = DATE '9999-12-31'
                LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_adm_cat adm_cat ON
                	adm_cat.dw_adm_key = adm.dw_adm_key
--	            LEFT JOIN ENTPR_BP_ADS_VIEWS.dsl_code_table adm_tos_cat_desc ON
--	            	adm_cat.tos_cat = adm_tos_cat_desc.CODE_CD 
--	            	AND adm_tos_cat_desc.COLUMN_NAME = 'tos_cat'
--	            	AND adm_tos_cat_desc.CODE_CLM_TYP ='Inpatient'               	
--	            LEFT JOIN ENTPR_BP_ADS_VIEWS.dsl_code_table adm_pos_cat_desc ON
--	            	adm_cat.pos_cat = adm_pos_cat_desc.CODE_CD 
--	            	AND adm_pos_cat_desc.COLUMN_NAME = 'pos_cat'
--	            	AND adm_pos_cat_desc.CODE_CLM_TYP ='Inpatient'      
	            	
                -- Institutional Inpatient Non-Admission Claims	
                LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_ipn_clm_li ipn ON
                    ipn.dw_clm_key = ck.dw_clm_key
                    and ipn.li_num = clm_li.li_num
                    and ipn.pd_thru_dt = DATE '9999-12-31'
                LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_ipn_cat ipn_cat ON
                	ipn_cat.dw_clm_key = ipn.dw_clm_key
--	            LEFT JOIN ENTPR_BP_ADS_VIEWS.dsl_code_table ipn_tos_cat_desc ON
--	            	ipn_cat.tos_cat = ipn_tos_cat_desc.CODE_CD 
--	            	AND ipn_tos_cat_desc.COLUMN_NAME = 'tos_cat'
--	            	AND ipn_tos_cat_desc.CODE_CLM_TYP ='Inpatient'       
--	            LEFT JOIN ENTPR_BP_ADS_VIEWS.dsl_code_table ipn_pos_cat_desc ON
--	            	ipn_cat.tos_cat = ipn_pos_cat_desc.CODE_CD 
--	            	AND ipn_pos_cat_desc.COLUMN_NAME = 'pos_cat'
--	            	AND ipn_pos_cat_desc.CODE_CLM_TYP ='Inpatient'   
	            	
                -- Professional Claims
                LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_proc_clm_li proc ON
                    proc.dw_clm_key = ck.dw_clm_key
                    and proc.li_num = clm_li.li_num
                    and proc.pd_thru_dt = DATE '9999-12-31'	
				LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_proc_cat proc_cat ON
					proc_cat.dw_clm_key = proc.dw_clm_key
					and proc_cat.li_num = proc.li_num
--	            LEFT JOIN ENTPR_BP_ADS_VIEWS.dsl_code_table proc_tos_cat_desc ON
--	            	proc_cat.tos_cat = proc_tos_cat_desc.CODE_CD 
--	            	AND proc_tos_cat_desc.COLUMN_NAME = 'tos_cat'
--	            	AND proc_tos_cat_desc.CODE_CLM_TYP ='Professional'      
--	            LEFT JOIN ENTPR_BP_ADS_VIEWS.dsl_code_table proc_pos_cat_desc ON
--	            	proc_cat.tos_cat = proc_pos_cat_desc.CODE_CD 
--	            	AND proc_pos_cat_desc.COLUMN_NAME = 'pos_cat'
--	            	AND proc_pos_cat_desc.CODE_CLM_TYP ='Professional'      
	            	
                WHERE		            
                	ck.disp_cd = 'A'
		            AND clm_li.disp_cd = 'A'
		            AND ck.source_schema_cd = 'IL'
		            AND ck.home_host_local_ind in ('HOME', 'LOCAL')		       
		    ) a
		) b
        LEFT JOIN ENTPR_BP_ADS_VIEWS.dsl_code_table tos_cat_desc ON
        	b.tos_cat_cd = tos_cat_desc.CODE_CD
        	AND b.DSL_CLM_TYP = tos_cat_desc.CODE_CLM_TYP
        	AND tos_cat_desc.COLUMN_NAME = 'tos_cat'
        	   
        LEFT JOIN ENTPR_BP_ADS_VIEWS.dsl_code_table pos_cat_desc ON
        	b.pos_cat_cd = pos_cat_desc.CODE_CD
        	AND b.DSL_CLM_TYP = pos_cat_desc.CODE_CLM_TYP
        	AND pos_cat_desc.COLUMN_NAME = 'pos_cat'
    )
    , mbr_info AS (
        SELECT
            mbr.dw_mbr_key
            , mbr.gndr_cd
            --, year(acrd.incurd_dt) - year(mbr.dob_dt) as age -- Not currently used. Create age buckets if needed in future.
            , zip.POSTL_CD as "mbr_zip"
            , zip.CITY_NAME as "mbr_city"
        FROM
            ENTPRIL_PRD_VIEWS_ALL.MBR mbr
        LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.MBR_ADDR MBR_ADDR on mbr.DW_MBR_KEY = mbr_addr.DW_MBR_KEY
            and MBR_ADDR.NOW_IND = 'Y'
            and MBR_ADDR.FNL_IND = 'Y'
            and MBR_ADDR.PRIMY_LOCN_ADDR_IND = 'Y'
            and MBR_ADDR.PRIMY_MAIL_ADDR_IND = 'Y'
        LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.ADDR maddr on maddr.DW_ADDR_KEY = MBR_ADDR.DW_ADDR_KEY
        LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.POSTL_REFR zip on maddr.DW_POSTL_KEY = zip.DW_POSTL_KEY
                                and zip.NOW_IND = 'Y' -- Extra policy info.
        WHERE
            mbr.NOW_IND = 'Y' and mbr.FNL_IND = 'Y'
        QUALIFY ROW_NUMBER() Over (PARTITION BY mbr.dw_mbr_key ORDER BY mbr_addr.rec_eff_dt DESC) = 1
    ), code_table as (
    SELECT
        code_cd
        ,column_name
        ,code_txt
        ,eff_date
        ,exp_date
    FROM ENTPRIL_PRD_VIEWS_ALL.CODE_TABLE
    WHERE
        column_name in(
        'HCPCS_CPT_CD',
        'DIAG_CD',
        'RVNU_CD'))

        SELECT
    --- Provider Info ---
            acrd.prov_fincl_id as "bill_pfin"
            , CASE
                WHEN acrd.prov_fincl_id IS NULL THEN ''
                WHEN LENGTH(acrd.prov_fincl_id) > 10 THEN RIGHT(acrd.prov_fincl_id, 10)
                ELSE acrd.prov_fincl_id
            END as "bill_pfin_10trimmed"
            , acrd.provider_payee_name
            , acrd.primy_prcg_prov_spclty_cd

    --- Member Info ---
            , acrd.dw_mbr_key
            , mbr_info.gndr_cd
            --, year(acrd.incurd_dt) - year(mbr.dob_dt) as age -- Not currently used. Create age buckets if needed in future.
            , mbr_info.mbr_zip
            , mbr_info.mbr_city
            , acct.acct_name

     --- Claim Info ---
            , acrd.dw_clm_cntrl_key
            , concat(to_char(acrd.dw_clm_key),'-',to_char(acrd.Li_num)) as "claim_line_key"
            , acrd.dw_clm_key
            , acrd.Li_num
            , acrd.incurd_dt
            , acrd.incurd_dt - EXTRACT(DAY from acrd.incurd_dt) + 1 as "incurd_month"
            , acrd.inpat_outpat_cd
            , acrd.HCPCS_CPT_Cd
            , cpt_code.code_txt as "HCPCS_CPT_Code_Desc"
            , acrd.rvnu_cd
            , rvcode.code_txt as "RevCD_Desc"
            , CASE
             WHEN cpt_code.code_txt is NULL OR cpt_code.code_txt = 'Not Available' THEN rvcode.code_txt
                ELSE cpt_code.code_txt
            END AS "HCPC_OR_REV_DESC"
            , diag.code_txt as "prim_diag" -- from acrd.primy_diag_cd
            , ccs2.diag_desc as "ICD-10-CM Codes Description"
            , ccs2.CCS_desc as "CCSR Category Description"
            , clmdrg.drg_cd
            , acrd.tos_cat_cd
            , acrd.tos_cat
            , acrd.pos_cat_cd
            , acrd.pos_cat
            --, plcy.FINCL_ARNGMT_CD -- commented out for now. need to solve duplication issues before including if needed in future
            --, plcy_code.code_txt as FINCL_ARNGMT_CD_Desc
            , acrd.billd_amt
            , acrd.prov_alwd_amt
            , acrd.net_elig_amt
            , acrd.net_elig_rd_amt
            , acrd.net_pd_rd_amt
            , CASE
                WHEN acrd.net_elig_rd_amt IS NULL then acrd.net_elig_amt
                ELSE acrd.net_pd_rd_amt
            END as Net_Elig_or_RD_pd -- Is this supposed to be the Allowed or Real Deal amount? Not prov_allwd_amnt
            ,acrd.Svc_To_Dt - acrd.Svc_From_Dt as LOS
        FROM
            --- Base RADAR tables ---
            all_claims_with_rd acrd
            LEFT JOIN RADAR.SG_CCS ccs2 on ccs2.diag = acrd.primy_diag_cd -- Code descriptions joined to RADAR views.

            LEFT JOIN code_table cpt_code on cpt_code.code_cd = acrd.hcpcs_cpt_cd
                and cpt_code.column_name = 'hcpcs_cpt_cd'
                and acrd.incurd_dt BETWEEN cpt_code.EFF_DATE and cpt_code.EXP_DATE
            LEFT JOIN code_table diag on diag.code_cd = acrd.primy_diag_cd
                and diag.column_name = 'diag_cd'
                and acrd.incurd_dt BETWEEN diag.EFF_DATE and diag.EXP_DATE
            LEFT JOIN code_table rvcode on rvcode.code_cd = acrd.rvnu_cd
                and rvcode.column_name = 'rvnu_cd'
                and acrd.incurd_dt BETWEEN rvcode.EFF_DATE and rvcode.EXP_DATE

            --- Base RADAR tables ---
            -- Additional Member Info
            LEFT JOIN mbr_info ON acrd.dw_mbr_key = mbr_info.dw_mbr_key
    -- Extra policy info.
    /*     -- Code block removed until further testing identifies how to join to MBR_PLCY without introducing duplication
                        -- This introduces possibility of duplication of lines where there are multiple active policies for a member
                        -- and the active policies have different FINCL_ARNGMT_CD.
                        LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.MBR_PLCY mbr_plcy on acrd.DW_MBR_KEY = mbr_plcy.DW_MBR_KEY
                            AND mbr_plcy.fnl_ind = 'Y'
                            and mbr_plcy.now_ind = 'Y'
                        LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.PLCY plcy on mbr_plcy.DW_PLCY_KEY = plcy.DW_PLCY_KEY
                            AND plcy.fnl_ind = 'Y'
                            AND plcy.prod_ln_cd = 'H'
                        LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.CODE_TABLE plcy_code on plcy_code.code_cd = plcy.FINCL_ARNGMT_CD
                            and plcy_code.column_name = 'FINCL_ARNGMT_CD'
                            and plcy_code.exp_date >= '2020-07-21'
    */
            -- Additional account info.
            LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.ACCT on acct.dw_acct_key = acrd.dw_acct_key
                and acct.now_ind = 'Y'
            -- Currently just getting the DRG Code.
            LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.CLM_DRG clmdrg ON clmdrg.DW_CLM_KEY = acrd.DW_CLM_KEY
                and clmdrg.DRG_TYP_CD = 'H';
'''
            print(f'loading pfin: {pfin} from {start_date} TO {end_date}')
            # Write recipe outputs
            e.query_to_df(query=insert_query)
            start_date = end_date
            end_date = start_date + delta
        except Exception as err:
            if 'No more spool space' in str(err):
                print('No more spool space error')
                delta = (end_date-start_date)/2

                if delta < timedelta(days=180):
                    raise RuntimeError("Query is too complex. Unable to load 6 months of data. Please refactor and try again.")

                end_date = start_date + delta
                continue
            else:
                raise err
end_time = datetime.now()
seconds = (end_time-start_time).seconds
print(f'Load process ended at {end_time} taking {seconds//3600} hours, {seconds//60%60} minutes, and {seconds%60} seconds')