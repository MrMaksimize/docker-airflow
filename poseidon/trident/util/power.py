class Power():
    def __init__(self, subscriptionKey):
        self.subscriptionKey = subscriptionKey
        self.baseurl = 'https://api.powerfactorscorp.com' 
        self.headers = {"Ocp-Apim-Subscription-Key": subscriptionKey}
        
    def getPlants(self):
        """
        Get id and name of all plants associated with this subscription key.
        
        Returns:
             json response list structured as [{'id': string, 'name': string}, ...]
        """
        planturl = self.baseurl + '/drive/v2/plant'
        r = requests.get(planturl, headers=self.headers)
        response = r.json()
        return response


    def initializeIDS(self, plantResponse):
        """Returns a dictionary mapping plant IDs to plant names"""
        plants = self.getPlants()
        nameDictionary = {}
        for response in plants:
            nameDictionary[response['id']] = response['name']
        return nameDictionary
    
    
    def getAttributes(self):
        """
        Get list of available plant attribute names. Currently all plants support the same attributes, 
        so just get attributes from first asset.
        """
        url = self.baseurl + f'/drive/v2/plant/{list(self.plants.keys())[0]}/attribute'
        r = requests.get(url, headers=self.headers)
        r = r.json()
        attr_names = []
        for attr in r:
            attr_names.append(attr['name'])
            
        return attr_names

    
    def getDateRanges(self, start_date, end_date):
        """
        Break the date range between start_date and end_date into 1 week intervals, if necessary, and
        convert to RFC3339 format: %Y-%m-%dT%H:%M:%SZ.
        
        API requires 'startTime' and 'endTime' request params to be within 1 week of each other and to be
        in RFC3339 format.
        
        Args:
            start_date (str): first day of time period (inclusive) in '%m/%d/%Y' format
            end_date (str): last day of time period (inclusive) in '%m/%d/%Y' format (can be the same as start_date)
        
        Returns:
            List of tuples containing startTime and endTime strings in RFC3339 format
        """
        start = dt.datetime.strptime(start_date, '%Y-%m-%d')
        end = dt.datetime.strptime(end_date, '%Y-%m-%d')

        dates = []
        rel_start = start
        a_week = dt.timedelta(days=7)
        a_day = dt.timedelta(hours=23, minutes=59, seconds=59)
        rfc3339_format = '%Y-%m-%d'
        remainder = end - rel_start

        while remainder > a_week:
            dates.append((rel_start.strftime(rfc3339_format), (rel_start+a_week+a_day).strftime(rfc3339_format)))
            rel_start += dt.timedelta(days=7)
            remainder = end - rel_start

        dates.append(((end-remainder).strftime(rfc3339_format), (end+a_day).strftime(rfc3339_format)))
        return dates 
    
    
    def get_data(self, start_date, end_date, elem_paths, attr, resolution="raw", fp=None):
        """
        Retrieve attribute data for the specified assets (plant) from the specified time period by 
        querying inincrements of 1 week or less.
        
        startTime (str): requested start date (inclusive), formatted as 'YYYY-mm-dd'
        endTime(str): requested end date (exclusive), formatted as 'YYYY-mm-dd'
        elem_paths (str list): IDs of elements to retrieve data from
        attr (str): name of attribute (e.g. 'AC_POWER', 'IRRADIANCE_POA', 'T_AMB', 'T_MOD', or 'WIND_SPEED')
            to retrieve data for
        resolution (str): time series data resolution, can be "day", "hour", or "raw"
        fp (str): file path to save csv of data to
        """
        dataURL = self.baseurl + '/drive/v2/data'
        results = {path: [] for path in elem_paths} # To store values for each element
        dates = self.getDateRanges(start_date, end_date)
        # Results needs to store a dict for each attribute--in format_reults, merge the dicts with index of timestamps 
        # to form a dataframe (a column of readings for each attribute)
        # For making the daterange for the index
        num_vals = 0
        start_tstamp = None
        end_tstamp = None
        for path in elem_paths: # Loop through each element in list
            for start, end in dates: # Iterate through list of start and end times
                body = {"startTime": start,
                        "endTime": end,
                        "resolution": resolution,
                        "attributes": attr,
                        "ids": path}
                r = requests.post(dataURL, headers=self.headers, data=body).json() # Use POST to avoid hitting max URL length w/ many params
                results[path] += r['assets'][0]['attributes'][0]['values'][1:] # Append readings for this time period to list of readings for this element
                if start_tstamp is None: # Get the start timestamp of the entire time period
                    start_tstamp = pd.to_datetime(r['assets'][0]['startTime'][:19])+dt.timedelta(minutes=5)  
                
            end_tstamp = pd.to_datetime(r['assets'][0]['endTime'][:19])
            num_vals = len(results[path])
       
        tstamps = pd.date_range(start_tstamp, end_tstamp, periods=num_vals)
        df_5min = pd.DataFrame(index=tstamps, data=results)
        df_15min = df_5min.resample('15T', label='right', closed='right').mean()
        
        if fp:
            df_15min.to_csv(fp)
            return
        else:
            return df_15min, df_5min
    
        # First reading returned is over the time period from (startTime-interval) to (startTime),
        # Last reading returned is over the time period from (endTime-interval) to (endTime),
        # i.e. readings are rolled forward -- timestamp corresponding to reading is that at
        # the end of the interval over which that reading was taken