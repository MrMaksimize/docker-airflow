SELECT *
FROM (
	SELECT 
		c.SP_SAPNO as "project_number",
		c.SP_PROJECT_NM as "title",
		c.SP_PROJECT_DESC as "project_description",
		c.SP_COUNCIL_DISTRICTS as "council_district",
		c.SP_COMMUNITY_PLANS as "community_plan",
		c.SP_ASSET_TYPE_DESC as "asset_type",
		c.SP_ASSET_TYPE_GROUP as "asset_group",

		c.SP_PROJECT_PHASE as "project_phase",	

		c.SP_CONTRACTOR_DESC as "contractor",
		c.SP_SENIOR_NM as "contact_person",

		c.SP_PRELIM_ENGR_START_DT as "prelim_engin_start",
		c.SP_AWARD_FINISH_DT as "award_const_contract",
		c.SP_DESIGN_FINISH_DT as "finish_design",
		c.SP_CONSTRUCTION_START_DT as "const_start",
		c.SP_CONSTR_FINISH_DT as "const_complete",
		c.SP_PROJECT_CLOSEOUT_DT as "project_closeout",


		c.SP_TOTAL_CONSTRUCTION_COST as "total_const_cost",
		c.SP_TOTAL_PROJECT_COST as "total_project_cost",
		c.SP_PROJECT_INFO_DESC as "funding_status",
		c.SP_FISCAL_YEAR as "fiscal_year",
		c.SP_DATA_DATE as "last_updated",
		c.SP_TYPE_CD as "type_ind",
		c.SP_UPDATE_DT,
		MAX(c.SP_UPDATE_DT) OVER (PARTITION BY c.SP_SAPNO) AS MAX_DATE

	FROM ECP.SP_PROJECT_CASH_FLOW c
	WHERE c.SP_PUBLIC = 'Y'
)

WHERE SP_UPDATE_DT = MAX_DATE