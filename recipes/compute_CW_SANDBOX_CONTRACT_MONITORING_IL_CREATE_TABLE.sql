 CREATE MULTISET TABLE RADAR.cw_sandbox_il_contract_monitoring ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      CLAIM_LINE_KEY VARCHAR(41) CHARACTER SET LATIN NOT CASESPECIFIC,
      dw_clm_key DECIMAL(18,0),
      provider_payee_name VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      dw_mbr_key DECIMAL(18,0),
      incurd_dt DATE FORMAT 'YY/MM/DD',
      li_num DECIMAL(4,0),
      hcpcs_cpt_cd CHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      HCPCS_CPT_Code_Desc VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
      RevCD_Desc VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
      HCPC_OR_REV VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
      rvnu_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      prov_alwd_amt DECIMAL(11,2),
      LOS INTEGER,
      Net_Elig_or_RD DECIMAL(15,2),
      bill_pfin VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      bill_pfin_10trimmed VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      primy_prcg_prov_spclty_cd CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      prim_diag VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
      "ICD-10-CM Codes Description" VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
      "CCSR Category Description" VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
      GNDR_CD CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      age INTEGER,
      mbr_zip VARCHAR(12) CHARACTER SET LATIN NOT CASESPECIFIC,
      mbr_city2 VARCHAR(28) CHARACTER SET LATIN NOT CASESPECIFIC,
      FINCL_ARNGMT_CD CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      FINCL_ARNGMT_CD_Desc VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
      ACCT_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      net_elig_rd_amt DECIMAL(15,2),
      CODE_TXT CHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
      DRG_CD CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC)
PRIMARY INDEX ( dw_clm_key ,li_num );