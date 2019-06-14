SELECT 
      p.agency_type 
      , p.call_category
      , p.City as 'address_city'
      , p.incident_Number as 'incident_number' 
      , p.Jurisdiction as 'jurisdiction'
      , p.problem as 'problem'
      ,p.Response_Date as 'date_response'
      ,p.State as 'address_state'
      ,p.zip as 'address_zip'
      ,p.response_day as 'day_response'
      ,p.response_month as 'month_response'
      ,p.response_year as 'year_response'
      
      
FROM Archive.dbo.ODP_IncidentsView as p  
  
where
      p.Jurisdiction = 'San Diego'
      --AND p.call_category != 'OTHER'
      AND p.response_year = YEAR(GETDATE())

ORDER BY p.Response_Date DESC