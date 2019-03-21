SELECT 
	p.agency_type, 
	p.City as 'city',
	p.Problem as 'problem',
	COUNT(p.Problem) as 'problem_count',
	p.response_month,
	p.response_year 

      
FROM Archive.dbo.ODP_ProblemNatureView as p 
WHERE 
	p.City = 'SAN DIEGO'
	/*AND p.response_year = YEAR(GETDATE())*/
	
GROUP BY p.Problem, p.response_month, p.agency_type, p.City, p.response_month, p.response_year
ORDER BY p.response_year, p.response_month