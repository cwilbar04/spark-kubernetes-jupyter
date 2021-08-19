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
 --         age INTEGER, -- commented out for now as not needed. consider age grouping if needed in future
            mbr_zip VARCHAR(12) CHARACTER SET LATIN NOT CASESPECIFIC,
            mbr_city VARCHAR(28) CHARACTER SET LATIN NOT CASESPECIFIC,
            ACCT_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,

            --- Claim Info ---
            claim_line_key VARCHAR(41) CHARACTER SET LATIN NOT CASESPECIFIC,
            dw_clm_key DECIMAL(18,0),
            li_num DECIMAL(4,0),
            incurd_dt DATE FORMAT 'YY/MM/DD',
            incurd_month DATE FORMAT 'YY/MM/DD',
            inpat_outpat_cd 
            hcpcs_cpt_cd CHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
            HCPCS_CPT_Code_Desc VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            rvnu_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
            RevCD_Desc VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            HCPC_OR_REV_DESC VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            prim_diag VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            "ICD-10-CM Codes Description" VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            "CCSR Category Description" VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            DRG_CD CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
--          CODE_TXT CHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC, -- DSL category. comment out futher testing needed
--          FINCL_ARNGMT_CD CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC, -- commented out for now. need to solve duplication issues before including if needed in future
--          FINCL_ARNGMT_CD_Desc VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
            billd_amt DECIMAL(11,2),
            prov_alwd_amt DECIMAL(11,2),
            net_elig_amt DECIMAL(15,2),
            net_elig_rd_amt DECIMAL(15,2),
            net_pd_rd_amt DECIMAL(15,2),
            Net_Elig_or_RD DECIMAL(15,2),
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
to_load

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
to_load['bill_pfin'] = to_load['bill_pfin'].astype(str).str.zfill(13)
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
                SELECT
                    DISTINCT
            --- Provider Info ---
                    prov.prov_fincl_id as "bill_pfin"
                    , CASE
                        WHEN prov.prov_fincl_id IS NULL THEN ''
                        WHEN LENGTH(prov.prov_fincl_id) > 10 THEN RIGHT(prov.prov_fincl_id, 10)
                        ELSE prov.prov_fincl_id
                    END as "bill_pfin_10trimmed"
                    , ck.provider_payee_name
                    , prov.primy_prcg_prov_spclty_cd

            --- Member Info ---
                    , ck.dw_mbr_key
                    , mbr.gndr_cd
                    --, year(ck.incurd_dt) - year(mbr.dob_dt) as age -- Not currently used. Create age buckets if needed in future.
                    , zip.POSTL_CD as "mbr_zip"
                    , zip.CITY_NAME as "mbr_city"
                    , acct.acct_name

             --- Claim Info ---
                    , concat(to_char(ck.dw_clm_key),'-',to_char(clm_li.Li_num)) as "claim_line_key"
                    , ck.dw_clm_key
                    , clm_li.Li_num
                    , ck.incurd_dt
                    , ck.incurd_dt - EXTRACT(DAY from ck.incurd_dt) + 1 as "incurd_month"
                    , clm_li.HCPCS_CPT_Cd
                    , cpt_code.code_txt as "HCPCS_CPT_Code_Desc"
                    , clm_li.rvnu_cd
                    , rvcode.code_txt as "RevCD_Desc"
                    , CASE
                        WHEN cpt_code.code_txt is NULL OR cpt_code.code_txt = 'Not Available' THEN rvcode.code_txt
                        ELSE cpt_code.code_txt
                    END AS "HCPC_OR_REV_DESC"
                    , diag.code_txt as "prim_diag" -- from clm_li.primy_diag_cd
                    , ccs2.diag_desc as "ICD-10-CM Codes Description"
                    , ccs2.CCS_desc as "CCSR Category Description"
                    , clmdrg.drg_cd
                    --, dsl.code_txt -- DSL category. comment out futher testing needed
                    --, plcy.FINCL_ARNGMT_CD -- commented out for now. need to solve duplication issues before including if needed in future
                    --, plcy_code.code_txt as FINCL_ARNGMT_CD_Desc
                    , clm_li.billd_amt
                    , clm_li.prov_alwd_amt
                    , clm_li.net_elig_amt
                    , rd.net_elig_rd_amt
                    , rd.net_pd_rd_amt
                    , CASE
                        WHEN rd.net_elig_rd_amt IS NULL then clm_li.net_elig_amt
                        ELSE rd.net_pd_rd_amt
                    END as Net_Elig_or_RD -- Is this supposed to be the Allowed or Real Deal amount? Not prov_allwd_amnt
                    ,clm_li.Svc_To_Dt - clm_li.Svc_From_Dt as LOS
                FROM
                    --- Base RADAR tables ---
                    "RADAR_VIEWS"."radardm_prod_claim" AS ck
                    INNER JOIN RADAR_VIEWS.radardm_prod_claim_line AS clm_li ON ck.dw_clm_key = clm_li.dw_clm_key
                        AND ck.source_schema_cd = clm_li.source_schema_cd
                    INNER JOIN RADAR_VIEWS.radardm_prod_provider prov on ck.billing_dw_prov_fincl_key = prov.dw_prov_fincl_key
                    LEFT JOIN RADAR.SG_CCS ccs2 on ccs2.diag = clm_li.primy_diag_cd -- Code descriptions joined to RADAR views.
                    LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.CODE_TABLE cpt_code on cpt_code.code_cd = clm_li.hcpcs_cpt_cd
                        and cpt_code.column_name = 'hcpcs_cpt_cd'
                    LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.CODE_TABLE diag on diag.code_cd = clm_li.primy_diag_cd
                        and diag.column_name = 'diag_cd'
                    LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.CODE_TABLE rvcode on rvcode.code_cd = clm_li.rvnu_cd
                        and rvcode.column_name = 'rvnu_cd'
                        and rvcode.exp_date >= '2020-07-21' -- why this date filter? There appears to be no expired codes.
                    --- Base RADAR tables ---
                    -- Additional Member Info
                    LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.MBR mbr on mbr.DW_MBR_KEY = ck.DW_MBR_KEY
                        and mbr.NOW_IND = 'Y'
                        and mbr.FNL_IND = 'Y'
                    LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.MBR_ADDR MBR_ADDR on MBR_ADDR.DW_MBR_KEY = ck.DW_MBR_KEY
                        and MBR_ADDR.NOW_IND = 'Y'
                        and MBR_ADDR.FNL_IND = 'Y'
                        and MBR_ADDR.PRIMY_LOCN_ADDR_IND = 'Y'
                        and MBR_ADDR.PRIMY_MAIL_ADDR_IND = 'Y'
                    LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.ADDR maddr on maddr.DW_ADDR_KEY = MBR_ADDR.DW_ADDR_KEY
                    LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.POSTL_REFR zip on maddr.DW_POSTL_KEY = zip.DW_POSTL_KEY
                        and zip.NOW_IND = 'Y' -- Extra policy info.
/*     -- Code block removed until further testing identifies how to join to MBR_PLCY without introducing duplication
                    -- This introduces possibility of duplication of lines where there are multiple active policies for a member
                    -- and the active policies have different FINCL_ARNGMT_CD.
                    LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.MBR_PLCY mbr_plcy on ck.DW_MBR_KEY = mbr_plcy.DW_MBR_KEY
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
                    LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.ACCT on acct.dw_acct_key = ck.dw_acct_key
                        and acct.now_ind = 'Y'
                    -- Needed to add additional conditions to join to remove duplication.
                    LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_visit rd on rd.dw_clm_key = ck.dw_clm_key
                        and clm_li.hcpcs_cpt_cd = rd.hcpcs_cpt_cd
                        and clm_li.svc_from_dt = rd.svc_from_dt
                        and clm_li.svc_to_dt = rd.svc_to_dt
                        and clm_li.pd_dt = rd.pd_dt
                        and clm_li.rvnu_cd = rd.rvnu_cd
                    LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_visit_cat cat on cat.dw_visit_key = rd.dw_visit_key
                    LEFT JOIN ENTPR_BP_ADS_VIEWS.dsl_code_table dsl on cat.tos_cat = dsl.code_cd
                        and dsl.column_name = 'tos_cat'
                    -- Currently just getting the DRG Code.
                    LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.CLM_DRG clmdrg ON clmdrg.DW_CLM_KEY = ck.DW_CLM_KEY
                        and clmdrg.DRG_TYP_CD = 'D'
                WHERE
                    ck.incurd_dt >= '{datetime.strftime(start_date,"%Y-%m-%d")}'
                    AND ck.incurd_dt < '{datetime.strftime(end_date,"%Y-%m-%d")}'
                    AND ck.disp_cd = 'A'
                    AND clm_li.disp_cd = 'A'
                    AND ck.source_schema_cd = 'IL'
                    AND ck.home_host_local_ind in ('HOME', 'LOCAL')
                    AND prov.prov_fincl_id = '{pfin}'  -- Really important to not use the trimmed calculation
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
                end_date = start_date + delta
                continue
            else:
                raise err
print(f'All data loaded')