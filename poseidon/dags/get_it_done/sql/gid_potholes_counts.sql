SELECT parent.SAP_Original_Notification_Number__c, Functional_Location__c, parent.Council_District__c, count(id)
FROM Case
WHERE
Status IN ('New','Acknowledged','Assigned','Duplicate')
AND SAP_Problem_Type__c IN ('Pothole','Minor Asphalt Repair')
AND parent.CreatedDate > 2016-05-20T22:23:59.000Z
AND parent.SAP_Original_Notification_Number__c != null
GROUP BY rollup (parent.SAP_Original_Notification_Number__c, Functional_Location__c, parent.Council_District__c) HAVING parent.Council_District__c != null
ORDER BY parent.Council_District__c, count(id) DESC
