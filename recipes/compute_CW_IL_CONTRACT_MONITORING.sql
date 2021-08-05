-- Call stored procedure to drop the table if it already exists
CALL  ${schema:CW_IL_CONTRACT_MONITORING}.DROP_TABLE_IF_EXISTS('${tbl:CW_IL_CONTRACT_MONITORING}');

--WITH ALL_CLAIMS AS (
CREATE MULTISET TABLE ${tbl:CW_IL_CONTRACT_MONITORING} AS (
    SELECT
        DISTINCT 
        concat(to_char(ck.dw_clm_key),'-',to_char(clm_li.Li_num)) as "CLAIM_LINE_KEY" 
    
    --- Base RADAR tables ---,
        ck.dw_clm_key,
        ck.provider_payee_name,
        ck.dw_mbr_key,
        ck.incurd_dt,
        clm_li.Li_num,
        clm_li.HCPCS_CPT_Cd,
        cpt_code.code_txt as HCPCS_CPT_Code_Desc,
        rvcode.code_txt as RevCD_Desc,
        CASE WHEN cpt_code.code_txt is NULL
        OR cpt_code.code_txt = 'Not Available' THEN rvcode.code_txt ELSE cpt_code.code_txt END AS "HCPC_OR_REV",
        clm_li.rvnu_cd,
        clm_li.prov_alwd_amt,
        clm_li.Svc_From_Dt - clm_li.Svc_To_Dt as LOS
        /*     , case when rd.net_elig_rd_amt IS NULL then clm_li.net_elig_amt
        		ELSE rd.net_pd_rd_amt END as Net_Elig_or_RD -- Is this supposed to be the Allowed or Real Deal amount? Not prov_allwd_amnt	 */,
        prov.prov_fincl_id as bill_pfin,
        CASE WHEN prov.prov_fincl_id IS NULL THEN '' WHEN LENGTH(prov.prov_fincl_id) > 10 THEN RIGHT(prov.prov_fincl_id, 10) ELSE prov.prov_fincl_id END as bill_pfin_10trimmed,
        prov.primy_prcg_prov_spclty_cd,
        diag.code_txt as prim_diag -- from clm_li.primy_diag_cd,
        ccs2.diag_desc as "ICD-10-CM Codes Description",
        ccs2.CCS_desc as "CCSR Category Description" --- Additional Member info,
        mbr.gndr_cd,
        2021 - year(mbr.dob_dt) as age -- Intentionally using 2021? Do we want a more accurate and dynamic DOB calculation using current date function and do we care about at least getting to the month level?,
        zip.POSTL_CD as mbr_zip,
        zip.CITY_NAME as mbr_city2 -- Extra policy info. -- potential for duplication here.,
        plcy.FINCL_ARNGMT_CD,
        plcy_code.code_txt as FINCL_ARNGMT_CD_Desc -- Extra account info.,
        acct.acct_name
        /*
        -- RD amount & category from DSL 
        	, rd.net_elig_rd_amt
        	, dsl.code_txt
        */
        -- Currently just getting the DRG Code.,
        clmdrg.drg_cd
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
        -- This introduces possibility of duplication of lines where there are multiple active policies for a member
        -- and the active policies have different FINCL_ARNGMT_CD.
        LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.MBR_PLCY mbr_plcy on ck.DW_MBR_KEY = mbr_plcy.DW_MBR_KEY
        AND mbr_plcy.fnl_ind = 'Y'
        and mbr_plcy.now_ind = 'Y'
        JOIN ENTPRIL_PRD_VIEWS_ALL.PLCY plcy on mbr_plcy.DW_PLCY_KEY = plcy.DW_PLCY_KEY
        AND plcy.fnl_ind = 'Y'
        AND plcy.prod_ln_cd = 'H'
        LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.CODE_TABLE plcy_code on plcy_code.code_cd = plcy.FINCL_ARNGMT_CD
        and plcy_code.column_name = 'FINCL_ARNGMT_CD'
        and plcy_code.exp_date >= '2020-07-21' -- Additional account info.
        LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.ACCT on acct.dw_acct_key = ck.dw_acct_key
        and acct.now_ind = 'Y'
        /* 
        -- RD?? Amount does not appear to be used in Tableau. 
        LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_visit rd on rd.dw_clm_key = ck.dw_clm_key 
        	and clm_li.hcpcs_cpt_cd = rd.hcpcs_cpt_cd
        LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_visit_cat cat on cat.dw_visit_key = rd.dw_visit_key	
        LEFT JOIN ENTPR_BP_ADS_VIEWS.dsl_code_table dsl
        	on cat.tos_cat = dsl.code_cd
        	and dsl.column_name = 'tos_cat' 
        */
        -- Currently just getting the DRG Code.
        LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.CLM_DRG clmdrg ON clmdrg.DW_CLM_KEY = ck.DW_CLM_KEY
        and clmdrg.DRG_TYP_CD = 'D'
    WHERE
        ck.incurd_dt BETWEEN '2019-01-01' AND '2020-12-31'
        AND ck.disp_cd = 'A'
        AND clm_li.disp_cd = 'A'
        AND ck.source_schema_cd IN ('IL')
        AND ck.home_host_local_ind in ('HOME', 'LOCAL')
        AND prov.prov_fincl_id = '0000000000331' -- Really important to not use the trimmed calculation
) WITH DATA PRIMARY INDEX(CLAIM_LINE_KEY);
