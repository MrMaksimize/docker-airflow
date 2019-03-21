SELECT CaseNumber, CreatedDate, Description, SAP_URL__c, Answer_1__c, Answer_2__c, Answer_3__c, Answer_4__c, Geolocation__Latitude__s, Geolocation__Longitude__s, Street_Address__c, Contact.FirstName, Contact.LastName, Contact.Phone, Contact.Email
FROM Case
WHERE Problem_Category__c IN('Abandoned Vehicle')
AND CreatedDate = LAST_N_DAYS:7
ORDER BY CreatedDate DESC
