 REPLACE VIEW ${tbl:RADAR_CONTRACT_MONITORING_VIEW} AS 
 WITH all_claims_with_rd AS (
        SELECT a.*
            ,sum(net_elig_proportion) OVER
                (PARTITION BY dw_clm_key, hcpcs_cpt_cd, svc_from_dt
                    , svc_to_dt, pd_dt, rvnu_cd ) as proportion_total
            ,ROW_NUMBER() OVER (PARTITION BY dw_clm_key, hcpcs_cpt_cd, svc_from_dt
                    , svc_to_dt, pd_dt, rvnu_cd
                    ORDER BY li_num) as line_number_order
            ,CASE
                WHEN line_number_order = 1 THEN net_elig_proportion + (1-proportion_total)
                ELSE net_elig_proportion
            END as actual_proportion
            ,net_pd_rd_amt_psi * actual_proportion as net_pd_rd_amt
            ,net_elig_rd_amt_psi * actual_proportion as net_elig_rd_amt
        FROM
        (
        SELECT
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
            , ck.incurd_dt
            , clm_li.inpat_outpat_cd
            , clm_li.svc_from_dt
            , clm_li.svc_to_dt
            , clm_li.pd_dt
            , clm_li.rvnu_cd
            , clm_li.HCPCS_CPT_Cd
            , clm_li.primy_diag_cd
--            , rd.tos_cat
            , ROUND(ZEROIFNULL(ZEROIFNULL(clm_li.net_elig_amt)/
                NULLIFZERO((sum(clm_li.net_elig_amt) OVER (
                PARTITION BY
                    clm_li.dw_clm_key, clm_li.hcpcs_cpt_cd, clm_li.svc_from_dt
                    , clm_li.svc_to_dt, clm_li.pd_dt, clm_li.rvnu_cd
                )))),2) as net_elig_proportion
            , rd.net_elig_rd_amt as net_elig_rd_amt_psi
            , rd.net_pd_rd_amt as net_pd_rd_amt_psi
            , clm_li.billd_amt
            , clm_li.prov_alwd_amt
            , clm_li.net_elig_amt
        FROM "RADAR_VIEWS"."radardm_prod_claim" AS ck
        INNER JOIN RADAR_VIEWS.radardm_prod_claim_line AS clm_li ON ck.dw_clm_key = clm_li.dw_clm_key
            AND ck.source_schema_cd = clm_li.source_schema_cd
        INNER JOIN RADAR_VIEWS.radardm_prod_provider prov on ck.billing_dw_prov_fincl_key = prov.dw_prov_fincl_key
        LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_visit rd ON
            rd.dw_clm_key = ck.dw_clm_key
            and clm_li.hcpcs_cpt_cd = rd.hcpcs_cpt_cd
            and clm_li.svc_from_dt = rd.svc_from_dt
            and clm_li.svc_to_dt = rd.svc_to_dt
         --   and clm_li.plc_of_trmnt_cd = rd.plc_of_trmnt_cd
         --   and clm_li.pd_dt = rd.pd_dt
            and clm_li.rvnu_cd = rd.rvnu_cd
            and rd.pd_thru_dt = '9999-12-31'
        WHERE
            ck.disp_cd = 'A'
            AND clm_li.disp_cd = 'A'
            --AND ck.incurd_dt >= '{datetime.strftime(start_date,"%Y-%m-%d")}'
            --AND ck.incurd_dt < '{datetime.strftime(end_date,"%Y-%m-%d")}'
            --AND ck.source_schema_cd = 'IL'
            AND ck.home_host_local_ind in ('HOME', 'LOCAL')
            --AND prov.prov_fincl_id = '{pfin}'  -- Really important to not use the trimmed calculation
         --PSI Tables have duplication where there are differing grvu_status. Use this qualification to insure the best "grvu_status" row is the only one retained
         QUALIFY ROW_NUMBER() OVER (PARTITION BY
                    clm_li.dw_clm_key, clm_li.li_num
                ORDER BY
                    CASE
                        WHEN grvu_status = 'Y' THEN 1
                        WHEN grvu_status = 'N' THEN 2
                        ELSE 3
                    END asc
                ) = 1
    ) a
    ), mbr_info AS (
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
            , 'not yet implemented' as tos_cat -- DSL category.
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
                and clmdrg.DRG_TYP_CD = 'D'