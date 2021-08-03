CREATE VIEW IL_CONTRACT_MONITORING 
SELECT
--- Base RADAR tables ---
	ck.dw_clm_key
	, ck.provider_payee_name
	, ck.dw_mbr_key
	, ck.incurd_dt
	
	, clm_li.Li_num
	, clm_li.HCPCS_CPT_Cd
	, cpt_code.code_txt as HCPCS_CPT_Code_Desc -- from clm_li.hcpcs_cpt_cd
	, rvcode.code_txt as RevCD_Desc -- from clm_li.rvnu_cd Code in Tableau defaults to this when HCPCS Code is Null. Can we move the logic here with generic column name? That way we can group by here for aggregation with less columns. Why is the CPT Code description ever NULL in the first place?	
	
	
	, clm_li.rvnu_cd
	, clm_li.prov_alwd_amt
	, clm_li.Svc_From_Dt-clm_li.Svc_To_Dt as LOS


	, case when rd.net_elig_rd_amt IS NULL then clm_li.net_elig_amt
		ELSE rd.net_pd_rd_amt END as Net_Elig_or_RD -- Is this supposed to be the Allowed or Real Deal amount? Not prov_allwd_amnt
		
	, prov.prov_fincl_id as bill_pfin
	, CASE WHEN prov.prov_fincl_id IS NULL THEN ''
		WHEN LENGTH(prov.prov_fincl_id) > 10 THEN RIGHT(prov.prov_fincl_id, 10) 
		ELSE prov.prov_fincl_id 
		END as bill_pfin_10trimmed
	, prov.primy_prcg_prov_spclty_cd
	
	, diag.code_txt as prim_diag -- from clm_li.primy_diag_cd
	
	, ccs2.diag_desc as "ICD-10-CM Codes Description" -- Confirmed 1:1 with Tableau using other data source
	, ccs2.CCS_desc as "CCSR Category Description" -- Confirmed 1:1 with Tableau using other data source	
	
--- Base RADAR tables ---

--- Member Zip Code & City Info - Used to generate map. Can this be added to the base RADAR tables?
--- Appears Member info is not intended to be included in RADAR tables. MBR table not used in creation.
--- OK to have separate. 
	, mbr.gndr_cd -- gndr_cd is directly on the CLM table representing point in time gender code. Can we add this to the RADAR tables or consider joining back to CLM table to get from here?
	, 2021-year(mbr.dob_dt) as age	-- Intentionally using 2021? Do we want a more accurate and dynamic DOB calculation using current date function and do we care about at least getting to the month level?
	-- There actually appears to be PAT_AGE on the Claim table directly. This is likely age at time of paid date. Is this the intended attribute? Can we add to RADAR or join back to the CLM table to get?
	--,
	
--- Member Zip Code & City Info - Used to generate map. Can this be added to the base RADAR tables?	
	, zip.POSTL_CD as mbr_zip
	, zip.CITY_NAME as mbr_city2
	
-- Extra policy info. Why not in base RADAR views? 
	-- Appears that a group was created but not used in Tableau. Is this still needed?
	, plcy.FINCL_ARNGMT_CD
	, plcy_code.code_txt as FINCL_ARNGMT_CD_Desc

-- Extra account info. Why not in base RADAR views?
	-- joins back to ACCT using ck.dw_acct_key where acct.now_ind = 'Y'
	, acct.acct_name	
	
-- Extra tax info. Is this 1:1 with PFIN? Or is there a different reason to include this?
--- This does inddeed appear to be 1:1. Can we drop?
	, tax.TAX_ID

-- RD?? Amount does not appear to be used in Tableau. What is this for? Is there supposed to be a formula for this
-- amount vs. net_elig_amt in the "Allowed or Real Deal" calculation in Tableau? Is that logic consistent and we can 
-- put it in the SQL query directly?
	, rd.net_elig_rd_amt

-- Cat?? Code Text?? Definitely could use a more descriptive name. Also does not appear to be used in Tableau.
-- Is this still needed? Large number of NULLs. Not really consistent with DRG description. Does have Surgery category
-- description however
	, dsl.code_txt

-- Currently just getting the DRG Code. Tableau has a "Sheet 1" datasource suggesting an excel data source mapping 
-- this code to DRG Desc Medicare & Base DRG Description which are used in Tableau
-- Do these mapping tables exist in a database somewhere? Do we need to create a policy around uploading these files?
-- Also there does not appear to be a mapping for every Drg Cd in this query (example Drg Cd = 18). Are we aware and
-- okay with this?
	,clmdrg.drg_cd


--****** Columns still to be parsed in to logical grouping based on joins below

FROM  
--- Base RADAR tables ---
RADAR_VIEWS.radardm_prod_claim AS ck 
LEFT JOIN RADAR_VIEWS.radardm_prod_claim_line AS clm_li
    ON ck.dw_clm_key = clm_li.dw_clm_key AND ck.source_schema_cd = clm_li.source_schema_cd
LEFT JOIN RADAR_VIEWS.radardm_prod_provider prov
	on ck.billing_dw_prov_fincl_key = prov.dw_prov_fincl_key
LEFT JOIN RADAR.SG_CCS ccs2 on ccs2.diag = clm_li.primy_diag_cd

-- Code descriptions joined to RADAR views. Is there a good reason to not include the descriptions in the RADAR views directly?
-- Should we make sure to include a date filter on all queries to the Code table?
-- Possibility to create duplicates if code exists multiple times in code table with differening Effective/End Dates
LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.CODE_TABLE cpt_code 
	on cpt_code.code_cd = clm_li.hcpcs_cpt_cd and cpt_code.column_name = 'hcpcs_cpt_cd'	
LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.CODE_TABLE diag 
	on diag.code_cd = clm_li.primy_diag_cd and diag.column_name = 'diag_cd'
LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.CODE_TABLE rvcode 
	on rvcode.code_cd = clm_li.rvnu_cd
		and rvcode.column_name = 'rvnu_cd'
		and rvcode.exp_date >= '2020-07-21' -- why this date filter? There appears to be no expired codes.
--- Base RADAR tables ---

-- Member most recent? gender & age(dob). Should this be included in the RADAR views? These should rarely change. 
LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.MBR mbr
	on mbr.DW_MBR_KEY = ck.DW_MBR_KEY
	and mbr.NOW_IND='Y'
	and mbr.FNL_IND='Y'

--- Member Zip Code & City Info - Used to generate map. Can this be added to the base RADAR tables?
LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.MBR_ADDR MBR_ADDR
	on MBR_ADDR.DW_MBR_KEY = ck.DW_MBR_KEY
	and MBR_ADDR.NOW_IND='Y'
	and MBR_ADDR.FNL_IND='Y'
	and MBR_ADDR.PRIMY_LOCN_ADDR_IND='Y'
	and MBR_ADDR.PRIMY_MAIL_ADDR_IND='Y'

LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.ADDR maddr
	on maddr.DW_ADDR_KEY = MBR_ADDR.DW_ADDR_KEY

LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.POSTL_REFR zip
	on maddr.DW_POSTL_KEY = zip.DW_POSTL_KEY
	and zip.NOW_IND='Y'

-- Extra policy info. Why not in base RADAR views? 
-- This introduces possibility of duplication of lines where there are multiple active policies for a member
-- and the active policies have different FINCL_ARNGMT_CD. 
-- This does not occur in this limited dataset but does appear as possible in the full dataset
-- Is this intentional? Or do we need to make sure we are only including one policy?
-- Is the INNER JOIN also intentional? There is duplication even with the INNER JOIN
-- Example 610040016
LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.MBR_PLCY mbr_plcy
	on ck.DW_MBR_KEY = mbr_plcy.DW_MBR_KEY 
	AND mbr_plcy.fnl_ind = 'Y'
	and mbr_plcy.now_ind = 'Y'		
JOIN ENTPRIL_PRD_VIEWS_ALL.PLCY plcy
    on mbr_plcy.DW_PLCY_KEY = plcy.DW_PLCY_KEY
    AND plcy.fnl_ind = 'Y'
	AND plcy.prod_ln_cd = 'H' 
LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.CODE_TABLE plcy_code on plcy_code.code_cd = plcy.FINCL_ARNGMT_CD
		and plcy_code.column_name = 'FINCL_ARNGMT_CD'
		and plcy_code.exp_date >= '2020-07-21'

-- Additional policy info. Why not in base RADAR views?
LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.ACCT on acct.dw_acct_key = ck.dw_acct_key
	and acct.now_ind = 'Y'

	
-- All to get the TAX_ID. Is it used anywhere in the analysis? Is this just a different identifier for PFIN?	
LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.PROV_FINCL billfin 
	on billfin.dw_prov_fincl_key = ck.billing_dw_prov_fincl_key 
	and billfin.NOW_IND ='Y'

LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.PROV_FINCL_TAX Tax_key
	ON billfin.dw_prov_fincl_key = Tax_key.dw_prov_fincl_key
	and Tax_key.now_ind = 'Y'
	
LEFT JOIN ENTPRIL_PRD_VIEWS_ALL.TAX
	ON Tax_Key.DW_TAX_KEY = Tax.DW_TAX_KEY
	and Tax.UPDT_SYS_CD = 'PRIME'

 
-- RD?? Amount does not appear to be used in Tableau. What is this for? Is there supposed to be a formula for this
-- amount vs. net_elig_amt in the "Allowed or Real Deal" calculation in Tableau? Is that logic consistent and we can 
-- put it in the SQL query directly?	
LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_visit rd on rd.dw_clm_key = ck.dw_clm_key 
	and clm_li.hcpcs_cpt_cd = rd.hcpcs_cpt_cd
	
-- Cat?? Code Text?? Definitely could use a more descriptive name. Also does not appear to be used in Tableau.
-- Is this still needed? Large number of NULLs. Not really consistent with DRG description. 
-- Does have Surgery category description, was/is this the intention of this column?
LEFT JOIN ENTPR_BP_ADS_PSI_VIEWS.il_visit_cat cat on cat.dw_visit_key = rd.dw_visit_key
	
LEFT JOIN ENTPR_BP_ADS_VIEWS.dsl_code_table dsl
	on cat.tos_cat = dsl.code_cd
	and dsl.column_name = 'tos_cat' 


-- Currently just getting the DRG Code. Tableau has a "Sheet 1" datasource suggesting an excel data source mapping 
-- this code to DRG Desc Medicare & Base DRG Description which are used in Tableau
-- Do these mapping tables exist in a database somewhere? Do we need to create a policy around uploading these files?
-- Also there does not appear to be a mapping for every Drg Cd in this query (example Drg Cd = 18). Are we aware and
-- okay with this?
LEFT JOIN CLM_DRG clmdrg ON clmdrg.DW_CLM_KEY = ck.DW_CLM_KEY
	and clmdrg.DRG_TYP_CD ='D'
	
WHERE 
	ck.incurd_dt BETWEEN '2019-01-01' AND '2020-12-31' 
	AND ck.disp_cd = 'A'
	AND clm_li.disp_cd = 'A'
	AND ck.source_schema_cd IN ('IL')
	AND ck.home_host_local_ind in ('HOME','LOCAL')
	AND prov.bill_pfin='0000000000331' 
	-- Really important to not use the trimmed calculation
;
