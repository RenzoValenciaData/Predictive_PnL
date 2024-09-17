
create or replace view  `dev-amer-analyt-actuals-svc-7a.amer_p_la_fin_data_hub.v_fct_pnl_predictive_historical_fit`  AS (

WITH PRD_RPT_LEVEL_RL0029 AS (
  SELECT 
    rpl_code
    ,CASE 
      WHEN aggregation = "Add" THEN 1
      ELSE -1
    END AS Aggregation_rpt_level
  FROM `prd-amer-analyt-datal-svc-88.amer_h_fit_mst.v_fit_reporting_level_hier_amer`
  WHERE level_04 = "RL0029"
)

,PRD_CHANNEL_CH01 AS (
  SELECT 
    channel_code
    ,level_02
    ,CASE 
      WHEN aggregation = "Add" THEN 1
      ELSE -1
    END AS Aggregation_Channel
  FROM `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_channel_hier_amer` 
  WHERE level_02 = "CH01"
)

,PRD_EVA_POST_ADJ AS (
  SELECT
    eva
    ,level_02
    ,CASE 
      WHEN aggregation = "Add" THEN 1
      ELSE -1
    END AS Aggregation_EVA
  FROM `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_eva_hier_amer` 
  WHERE level_02 = "Post_Adj"
)

,PRD_ICP_ZERO_ICP AS (
  SELECT 
  icp_code
  ,level_04
  ,CASE 
      WHEN aggregation = "Add" THEN 1
      ELSE -1
    END AS Aggregation_ICP
  FROM `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_icp_hier_amer` 
  WHERE level_04 = "ZERO_ICP"
)

,PRD_PRODUCT_HIERARCHY_BSP AS (
  SELECT
    DISTINCT sub_brand  
    ,bws AS bsp_code
    ,bws_desc AS bsp_desc
  FROM `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_prod_hier_sku_amer`
)

,PRD_ACCOUNT_HIERARCHY AS (
  SELECT
    account_code
    ,account_description AS account_desc
    ,level_04_description
    ,level_05_description
    ,level_06_description
    ,level_07_description  
  FROM `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_account_hier_amer` 
  WHERE level_03 = "PL229099"
    OR (level_03 = "Memo_Accounts" AND account_code = "PLV959999")
)

,PRD_ENTITY_HIERARCHY AS (
  SELECT 
    entity
    ,description as entity_desc
  FROM `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_entity_hier_amer` 
  WHERE ENTITY NOT LIKE '%ELIM%'
    AND ENTITY NOT LIKE '%_CAP%'
)

,ACCOUNT_H_SIGN AS (
  SELECT
    account_code
    ,aggregation_state as aggregation_account
  FROM `dev-amer-analyt-actuals-svc-7a.amer_p_la_fin_data_hub.account_h` 
)

,PRD_FIT_ENTITY_TO_SAC_ENTITY AS (
  SELECT 
    FIT_ENTITY
    ,sac_entity
    ,sac_entity_desc 
  FROM `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fdl_fit_sac_country_mapping_amer`
)

,FIT_PCL_PC_DATA AS (
  SELECT 
    account
    ,channel
    ,currency
    ,entity
    ,product
    ,icp
    ,scenario
    ,rptlevel
    ,PARSE_DATE('%Y-%m-%d', CONCAT(year, '-', -- date based on year, period
          CASE period
              WHEN 'Jan' THEN '01'
              WHEN 'Feb' THEN '02'
              WHEN 'Mar' THEN '03'
              WHEN 'Apr' THEN '04'
              WHEN 'May' THEN '05'
              WHEN 'Jun' THEN '06'
              WHEN 'Jul' THEN '07'
              WHEN 'Aug' THEN '08'
              WHEN 'Sep' THEN '09'
              WHEN 'Oct' THEN '10'
              WHEN 'Nov' THEN '11'
              WHEN 'Dec' THEN '12'
          END, '-01')) AS date
    ,eva
    ,version
    ,SUM(value) AS Value
  FROM `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_pcl_pc_amer` 
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)

,FIT_PCL_PC_RIGHT_SIGN AS (

  SELECT
    pc.account
    ,pc.channel
    ,pc.currency
    ,pc.entity
    ,pc.product
    ,pc.icp
    ,pc.scenario
    ,pc.rptlevel
    ,pc.date
    ,pc.eva
    ,pc.version
    ,pc.value
    ,pc.Value * pc.aggregation_rpt_level * pc.aggregation_channel * pc.aggregation_eva * pc.aggregation_icp * pc.aggregation_account AS Value_LCL
    
    FROM (
      SELECT 
        pc.*
        ,rpt.aggregation_rpt_level
        ,ch.aggregation_channel
        ,eva.aggregation_eva
        ,icp.aggregation_icp
        ,acc.aggregation_account
      FROM  FIT_PCL_PC_DATA pc
      LEFT JOIN PRD_RPT_LEVEL_RL0029 rpt ON pc.rptlevel = rpt.rpl_code
      LEFT JOIN PRD_CHANNEL_CH01 ch ON pc.channel = ch.channel_code
      LEFT JOIN PRD_EVA_POST_ADJ  eva ON pc.eva = eva.eva
      LEFT JOIN PRD_ICP_ZERO_ICP icp ON pc.icp = icp.icp_code
      LEFT JOIN ACCOUNT_H_SIGN acc ON pc.account = acc.account_code
    ) pc
)

-- Create Table for P&L Predictive --

,FIT_PC_BASE_ACCOUNTS_IN_COLUMNS AS (
  SELECT
    pc.account,
    pc.channel,
    pc.currency,
    pc.entity,
    pc.product,
    pc.bsp_code,
    pc.icp,
    pc.scenario,
    pc.rptlevel,
    pc.date,
    pc.eva,
    pc.version,
    IF(pc.level_04_description = "Volume Accounts", pc.Value_LCL, 0) AS Volume,
    IF(pc.level_04_description = "Total Overhead", pc.Value_LCL, 0) AS Overheads,
    IF(pc.level_04_description = "Other (Income) / Expense", pc.Value_LCL, 0) AS OIE,
    IF(pc.level_05_description = "Advertising, Consumer and Other Promotions", pc.Value_LCL, 0) AS A_and_C,
    IF(pc.level_06_description = "Cost of Goods Sold", pc.Value_LCL, 0) AS COGS,
    IF(pc.level_07_description = "Total Gross Sales", pc.Value_LCL, 0) AS Gross_Sales,
    IF(pc.level_07_description = "Revenue Reductions", pc.Value_LCL, 0) AS G2N
  FROM (
    SELECT 
      pc.*,
      ah.level_04_description,
      ah.level_05_description,
      ah.level_06_description,
      ah.level_07_description,
      ph.bsp_code
    FROM FIT_PCL_PC_RIGHT_SIGN pc
    LEFT JOIN PRD_ACCOUNT_HIERARCHY ah ON ah.account_code = pc.account
    LEFT JOIN PRD_PRODUCT_HIERARCHY_BSP ph ON ph.sub_brand = pc.product
  ) pc
)

,FIT_PC_NR_GP_OI_TABLE AS (
  SELECT 
    *
    ,Gross_Sales + G2N AS Net_Revenue
    ,Gross_Sales + G2N + COGS  AS Gross_Profit
    ,Gross_Sales + G2N + COGS + A_and_C + Overheads + OIE AS Operating_Income
  FROM FIT_PC_BASE_ACCOUNTS_IN_COLUMNS
)

,FIT_PC_FINAL_TABLE AS (
  SELECT
      date,
      entity,
      bsp_code,
      Account,
      value
  FROM
      (SELECT
          date,
          entity,
          bsp_code,
          SUM(Volume) AS PLV959999,
          -- SUM(Gross_Sales) AS PL201010,
          -- SUM(G2N) AS G2N,
          SUM(Net_Revenue) AS PL202099,
          SUM(Gross_Profit) AS PL213999,
          SUM(A_and_C) AS PL214099,
          SUM(Overheads) AS PL216099,
          SUM(OIE) AS PL223099,
          SUM(Operating_Income) AS PL229099
      FROM FIT_PC_NR_GP_OI_TABLE
      WHERE entity IN (SELECT ENTITY FROM PRD_ENTITY_HIERARCHY) --("BRLFS", "BRALLOC")
          AND scenario = "Act"
          AND currency = "LCL"
      GROUP BY date, entity, bsp_code
      ) AS SourceTable
  UNPIVOT
      (value FOR Account IN (PLV959999, PL202099, PL213999, PL214099, PL216099, PL223099, PL229099)
      ) AS UnpivotedTable
  WHERE value != 0
  ORDER BY date ASC
)

SELECT
  pc.date
  -- ,pc.entity as entity_code
  -- ,ent.entity_desc
  ,CASE pc.entity
    WHEN "MEXALLOC" THEN "MXALLOC"
    ELSE IFNULL(entF2S.sac_entity, PC.entity)
  END AS  entity_code
  ,CASE pc.entity 
    WHEN "MEXALLOC" THEN "Mexico HQ Allocations"
    ELSE IFNULL(entF2S.sac_entity_desc, ent.entity_desc)
  END AS  entity_desc
  ,pc.bsp_code
  ,prd.bsp_desc 
  ,pc.account as account_code
  ,CASE acc.account_desc
    WHEN "Net Revenues" THEN "Net Revenue"
    WHEN "Total Volume" THEN "Volume"
    WHEN "Segment Operating Income" THEN "OI"
    ELSE acc.account_desc
  END AS account_desc
  ,SUM(
    CASE 
      WHEN pc.account IN ("PL214099","PL216099","PL223099") THEN -1 * Value
      ELSE Value
    END
  ) as value_act
FROM FIT_PC_FINAL_TABLE pc
LEFT JOIN (
  SELECT DISTINCT bsp_code, bsp_desc
  FROM PRD_PRODUCT_HIERARCHY_BSP
) prd ON pc.bsp_code = prd.bsp_code
LEFT JOIN PRD_ACCOUNT_HIERARCHY acc ON pc.account = acc.account_code
LEFT JOIN PRD_ENTITY_HIERARCHY ent ON pc.entity = ent.entity
LEFT JOIN PRD_FIT_ENTITY_TO_SAC_ENTITY entF2S ON pc.entity = entF2S.fit_entity
WHERE pc.entity NOT IN ("ARLFS", "KICFSLAH", "LATOPUSD", "MBSCRLFS")
GROUP BY 1,2,3,4,5,6,7

)
