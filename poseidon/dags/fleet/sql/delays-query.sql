SELECT LOC_WORK_ORDER_LOC as work_loc,
WORK_ORDER_YR as work_order_yr,
WORK_ORDER_NO as work_order_no,
EQ_EQUIP_NO as equip_id,
DELAY_START_DATETIME as date_delay_start,
DELAY_END_DATETIME as date_delay_end,
DELAY_HOURS as delay_hours,
WDL_WORK_DELAY_CODE as work_delay_code,
DELAY_STATUS as delay_status
FROM EMSDBA.DLY_MAIN