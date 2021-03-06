SELECT 
DBA_HT.ACCOUNT_KEY,
DBA_HT.DBA_NAME
FROM
(SELECT
DOING_BUS_AS_NAME.DOING_BUS_AS_NAME AS DBA_NAME,
DOING_BUS_AS_NAME.ACCOUNT_KEY,
ROW_NUMBER() OVER (PARTITION BY ACCOUNT_KEY ORDER BY EFFECTIVE_FROM_DT DESC) AS ROW_NO
FROM DOING_BUS_AS_NAME
WHERE PRIMARY_DBA_IND = 'Y') DBA_HT
WHERE DBA_HT.ROW_NO = 1