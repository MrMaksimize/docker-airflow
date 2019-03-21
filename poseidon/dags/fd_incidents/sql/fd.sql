SELECT 
      p.agency_type 
      , p.call_category
      , p.City as 'city'
      , p.incident_Number as 'incident_number' 
      , p.Jurisdiction as 'jurisdiction'
      , p.problem as 'problem'
      ,p.Response_Date as 'response_date'
      ,p.State as 'state'
      ,p.zip as 'zip_code'
      ,p.response_day as 'response_day'
      ,p.response_month as 'response_month'
      ,p.response_year as 'response_year'
      
      
FROM Archive.dbo.ODP_IncidentsView as p  
  
where
      p.Jurisdiction = 'San Diego'
      --AND p.call_category != 'OTHER'
      AND p.response_year = YEAR(GETDATE())
      
ORDER BY p.Response_Date DESC