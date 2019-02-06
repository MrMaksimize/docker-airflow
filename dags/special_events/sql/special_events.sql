DECLARE @dateStart date;

SET @dateStart = DATEADD(MONTH, -24, GETDATE())

SELECT
    ev.EVENT_TITLE_NM as 'event_title'
    ,act.ACTIVITY_SEQ_NUM as 'event_id'
    ,act.SECONDARY_TITLE_TXT as 'event_subtitle'
    ,act.ACTIVITY_TYPE_CD as 'event_type'
    ,act.ACTIVITY_DESC as 'event_desc'
    ,act.LOCATION_TXT as 'event_loc'
    ,act.EVENT_START_DT as 'event_start'
    ,act.EVENT_END_DT as 'event_end'
    ,act.EXPECTED_ATTENDANCE_NUM as 'exp_attendance'
    ,act.EXPECTED_PARTICIPANTS_NUM as 'exp_participants'
    ,act.HOST_NM as 'event_host'
    ,act.ACTIVITY_URL as 'event_url'
    ,act.PIN_ADDRESS as 'event_address'
FROM secal.dbo.ACTIVITY act
LEFT OUTER JOIN secal.dbo.EVENT ev
ON act.EVENT_SEQ_NUM = ev.EVENT_SEQ_NUM
WHERE act.PUBLIC_VISIBILITY_IND = 'Y'
AND act.EVENT_START_DT >= @dateStart
ORDER BY act.EVENT_START_DT desc
