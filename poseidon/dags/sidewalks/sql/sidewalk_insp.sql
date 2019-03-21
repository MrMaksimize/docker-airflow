SELECT * 
FROM (
	SELECT gen.segmentid as sap_id,
	gen.id as legacy_id,
	con.rating,
	con.condition,
	con.cgLastModified,
	con.inspectiondate,
	MAX(con.inspectiondate) OVER (PARTITION BY gen.segmentid, gen.id) as MaxInspect,
	MAX(con.cgLastModified) OVER (PARTITION BY gen.segmentid, gen.id) as MaxMod
	FROM
	[dbo].sidemaingeneral gen
	LEFT OUTER JOIN [dbo].sideinspections con ON gen.sidemaingeneraloid = con.sidemaingeneraloid ) sq

WHERE cgLastModified = MaxMod and inspectiondate = MaxInspect