import pandas as pd
import requests
import json
import math
import logging
from datetime import datetime


forms = {'460A':'0','460B1':'12','460C':'1','460D':'5','497P1':'20','496':'19'}
payload = {'format':'json'}

# Set up date filtering
datesSheet = 'https://docs.google.com/spreadsheets/d/1Xv1qSWLpA0oL8AfsPJat6opTj0ppobgr69Y2NM4cMHU/pub?gid=0&single=true&output=csv'
deadlines = pd.read_csv(datesSheet, delimiter=',')
year = 2017
currentDeadlines = deadlines[(deadlines['year'] == 2016) & (deadlines['form'] == '460')]
currentDeadlines['deadline'] = pd.to_datetime(currentDeadlines['deadline'])
currentDeadlines.sort_values(['deadline'],inplace=True)
currentDeadlines['deadline_next'] = currentDeadlines['deadline'].shift(-1)
today = datetime.strptime('2016112',"%Y%m%d").date()
lastDeadline = currentDeadlines[(today > currentDeadlines['deadline']) & (today <= currentDeadlines['deadline_next'])]

outputDF = pd.DataFrame()
dateResponse = 'dateTK'
prod_columns = ['form',
  'schedule',
  'schedule_description',
  'recipient_id',
  'recipient_name',
  'filing_date',
  'report_period_from',
  'report_period_to',
  'contributor_code',
  'contributor_last',
  'contributor_first',
  'contributor_city',
  'contributor_state',
  'contributor_zip',
  'contributor_emp',
  'contributor_occ',
  'contribution_date',
  'contribution_amount',
  'contribution_annual',
  'contribution_desc',
  'contributor_id',
  'intermediary_last',
  'intermediary_first',
  'intermediary_city',
  'intermediary_state',
  'intermediary_zip',
  'intermediary_emp',
  'intermediary_occ',
  'filing_id']


def get_460A:
  countRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/transaction/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':'0',
                            'PageSize':'1',
                            'TransactionType':forms['460A'],
                            'ShowSuperceded':'false'})
  formTransactions = countRequest.json()['totalMatchingCount']
  print(formTransactions)

  if formTransactions < 1000:
      requestLoops = 1
  else:
      requestLoops = math.ceil(formTransactions/1000)
      
  transactionsList = []

  for i in range(requestLoops):
      page = str(i)
      transactionsRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/transaction/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':page,
                            'PageSize':'1000',
                            'TransactionType':forms['460A'],
                            'ShowSuperceded':'false'})
      allTransactions = transactionsRequest.json()['results']
      
      for t in allTransactions:
          # Create row
          transactionsList.append(['460', #form
                                       t['form_Type'], #schedule
                                       'Monetary contributions', #schedule_description
                                       t['filerStateId'], #recipient_id
                                       t['filerName'], #recipient_name
                                       dateResponse, #filing_date
                                       t['filingStartDate'], #'report_period_from'
                                       t['filingEndDate'], #'report_period_to'
                                       t['entity_Cd'], #'contributor_code'
                                       t['tran_NamL'], #'contributor_last'
                                       t['tran_NamF'], #'contributor_first'
                                       t['tran_City'], #'contributor_city'
                                       t['tran_ST'], #'contributor_state'
                                       t['tran_Zip4'], #'contributor_zip'
                                       t['tran_Emp'], #'contributor_emp'
                                       t['tran_Occ'], #'contributor_occ'
                                       t['tran_Date'], #'contribution_date'
                                       t['tran_Amt1'], #'contribution_amount'
                                       t['tran_Amt2'], #'contribution_annual'
                                       t['tran_Dscr'], #'contribution_desc'
                                       t['cmte_Id'], #'contributor_id'
                                       t['intr_NamL'], #'intermediary_last'
                                       t['intr_NamF'], #'intermediary_first'
                                       t['intr_City'], #'intermediary_city'
                                       t['intr_ST'], #'intermediary_state'
                                       t['intr_Zip4'], #'intermediary_zip'
                                       t['intr_Emp'], #'intermediary_emp'
                                       t['intr_Occ'], #'intermediary_occ'
                                       t['filingId']
                                       ])


  campaignTransactions = pd.DataFrame(transactionsList,columns=prod_columns)        
  outputDF = pd.concat([outputDF, campaignTransactions], ignore_index=True)

def get_460B1:
  countRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/transaction/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':'0',
                            'PageSize':'1',
                            'TransactionType':forms['460B1'],
                            'ShowSuperceded':'false'})
  formTransactions = countRequest.json()['totalMatchingCount']
  print(formTransactions)

  if formTransactions < 1000:
      requestLoops = 1
  else:
      requestLoops = math.floor(formTransactions/1000)
      
  transactionsList = []

  for i in range(requestLoops):
      page = str(i)
      transactionsRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/transaction/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':page,
                            'PageSize':'1000',
                            'TransactionType':forms['460B1'],
                            'ShowSuperceded':'false'})
      allTransactions = transactionsRequest.json()['results']
      
      for t in allTransactions:
          # Create row
          transactionsList.append(['460', #form
                                       t['form_Type'], #schedule
                                       'Loans', #schedule_description
                                       t['filerStateId'], #recipient_id
                                       t['filerName'], #recipient_name
                                       dateResponse, #filing_date
                                       t['filingStartDate'], #'report_period_from'
                                       t['filingEndDate'], #'report_period_to'
                                       t['entity_Cd'], #'contributor_code'
                                       t['tran_NamL'], #'contributor_last'
                                       t['tran_NamF'], #'contributor_first'
                                       t['tran_City'], #'contributor_city'
                                       t['tran_ST'], #'contributor_state'
                                       t['tran_Zip4'], #'contributor_zip'
                                       t['tran_Emp'], #'contributor_emp'
                                       t['tran_Occ'], #'contributor_occ'
                                       t['tran_Date'], #'contribution_date'
                                       t['tran_Amt1'], #'contribution_amount'
                                       t['tran_Amt2'], #'contribution_annual'
                                       t['tran_Dscr'], #'contribution_desc'
                                       t['cmte_Id'], #'contributor_id'
                                       t['intr_NamL'], #'intermediary_last'
                                       t['intr_NamF'], #'intermediary_first'
                                       t['intr_City'], #'intermediary_city'
                                       t['intr_ST'], #'intermediary_state'
                                       t['intr_Zip4'], #'intermediary_zip'
                                       t['intr_Emp'], #'intermediary_emp'
                                       t['intr_Occ'], #'intermediary_occ'
                                       t['filingId']
                                       ])


  campaignTransactions = pd.DataFrame(transactionsList,columns=prod_columns)        
  outputDF = pd.concat([outputDF, campaignTransactions], ignore_index=True)

def get_460C:
  countRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/transaction/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':'0',
                            'PageSize':'1000',
                            'TransactionType':forms['460C'],
                            'ShowSuperceded':'false'})
  formTransactions = countRequest.json()['totalMatchingCount']
  print(formTransactions)

  if formTransactions < 1000:
      requestLoops = 1
  else:
      requestLoops = math.floor(formTransactions/1000)
      
  transactionsList = []

  for i in range(requestLoops):
      page = str(i)
      transactionsRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/transaction/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':page,
                            'PageSize':'1000',
                            'TransactionType':forms['460C'],
                            'ShowSuperceded':'false'})
      allTransactions = transactionsRequest.json()['results']
      
      for t in allTransactions:
          # Create row
          transactionsList.append(['460', #form
                                       t['form_Type'], #schedule
                                       'Non monetary contributions', #schedule_description
                                       t['filerStateId'], #recipient_id
                                       t['filerName'], #recipient_name
                                       dateResponse, #filing_date
                                       t['filingStartDate'], #'report_period_from'
                                       t['filingEndDate'], #'report_period_to'
                                       t['entity_Cd'], #'contributor_code'
                                       t['tran_NamL'], #'contributor_last'
                                       t['tran_NamF'], #'contributor_first'
                                       t['tran_City'], #'contributor_city'
                                       t['tran_ST'], #'contributor_state'
                                       t['tran_Zip4'], #'contributor_zip'
                                       t['tran_Emp'], #'contributor_emp'
                                       t['tran_Occ'], #'contributor_occ'
                                       t['tran_Date'], #'contribution_date'
                                       t['tran_Amt1'], #'contribution_amount'
                                       t['tran_Amt2'], #'contribution_annual'
                                       t['tran_Dscr'], #'contribution_desc'
                                       t['cmte_Id'], #'contributor_id'
                                       t['intr_NamL'], #'intermediary_last'
                                       t['intr_NamF'], #'intermediary_first'
                                       t['intr_City'], #'intermediary_city'
                                       t['intr_ST'], #'intermediary_state'
                                       t['intr_Zip4'], #'intermediary_zip'
                                       t['intr_Emp'], #'intermediary_emp'
                                       t['intr_Occ'], #'intermediary_occ'
                                       t['filingId']
                                       ])


  campaignTransactions = pd.DataFrame(transactionsList,columns=prod_columns)        
  outputDF = pd.concat([outputDF, campaignTransactions], ignore_index=True)       


def get_497P1:
  countRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/transaction/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':'0',
                            'PageSize':'1',
                            'TransactionType':forms['497P1'],
                            'ShowSuperceded':'false'})
  formTransactions = countRequest.json()['totalMatchingCount']
  print(formTransactions)

  if formTransactions < 1000:
      requestLoops = 1
  else:
      requestLoops = math.floor(formTransactions/1000)
      
  transactionsList = []

  for i in range(requestLoops):
      page = str(i)
      transactionsRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/transaction/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':page,
                            'PageSize':'1000',
                            'TransactionType':forms['497P1'],
                            'ShowSuperceded':'false'})
      allTransactions = transactionsRequest.json()['results']
      
      for t in allTransactions:
        if t['tran_Date'] > lastDeadline:
          # Create row
          transactionsList.append(['460', #form
                                       ' ', #schedule
                                       '24-hr contribution report', #schedule_description
                                       t['filerStateId'], #recipient_id
                                       t['filerName'], #recipient_name
                                       dateResponse, #filing_date
                                       t['filingStartDate'], #'report_period_from'
                                       t['filingEndDate'], #'report_period_to'
                                       t['entity_Cd'], #'contributor_code'
                                       t['tran_NamL'], #'contributor_last'
                                       t['tran_NamF'], #'contributor_first'
                                       t['tran_City'], #'contributor_city'
                                       t['tran_ST'], #'contributor_state'
                                       t['tran_Zip4'], #'contributor_zip'
                                       t['tran_Emp'], #'contributor_emp'
                                       t['tran_Occ'], #'contributor_occ'
                                       t['tran_Date'], #'contribution_date'
                                       t['tran_Amt1'], #'contribution_amount'
                                       t['tran_Amt2'], #'contribution_annual'
                                       t['tran_Dscr'], #'contribution_desc'
                                       t['cmte_Id'], #'contributor_id'
                                       t['intr_NamL'], #'intermediary_last'
                                       t['intr_NamF'], #'intermediary_first'
                                       t['intr_City'], #'intermediary_city'
                                       t['intr_ST'], #'intermediary_state'
                                       t['intr_Zip4'], #'intermediary_zip'
                                       t['intr_Emp'], #'intermediary_emp'
                                       t['intr_Occ'], #'intermediary_occ'
                                       t['filingId']
                                       ])


  campaignTransactions = pd.DataFrame(transactionsList,columns=prod_columns)        
  outputDF = pd.concat([outputDF, campaignTransactions], ignore_index=True)


def get_460D:
  countRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/transaction/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':'0',
                            'PageSize':'1',
                            'TransactionType':forms['460D'],
                            'ShowSuperceded':'false'})
  formTransactions = countRequest.json()['totalMatchingCount']
  print(formTransactions)

  if formTransactions < 1000:
      requestLoops = 1
  else:
      requestLoops = math.floor(formTransactions/1000)
      
  transactionsList = []

  for i in range(requestLoops):
      page = str(i)
      transactionsRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/transaction/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':page,
                            'PageSize':'1000',
                            'TransactionType':forms['460D'],
                            'ShowSuperceded':'false'})
      allTransactions = transactionsRequest.json()['results']
      
      for t in allTransactions:
          if t['sup_Opp_Cd'] == 'S':
              # Create row
              transactionsList.append(['460', #form
                                       t['form_Type'], #schedule
                                       'Independent expenditures in support', #schedule_description
                                       t['cmte_Id'], #recipient_id
                                       fullName, #recipient_name
                                       dateResponse, #filing_date
                                       t['filingStartDate'], #'report_period_from'
                                       t['filingEndDate'], #'report_period_to'
                                       ' ', #'contributor_code'
                                       t['filerName'], #'contributor_last'
                                       ' ', #'contributor_first'
                                       ' ', #'contributor_city'
                                       ' ', #'contributor_state'
                                       ' ', #'contributor_zip'
                                       ' ', #'contributor_emp'
                                       ' ', #'contributor_occ'
                                       t['tran_Date'], #'contribution_date'
                                       t['tran_Amt1'], #'contribution_amount'
                                       t['tran_Amt2'], #'contribution_annual'
                                       t['tran_Dscr'], #'contribution_desc'
                                       t['filerStateId'], #'contributor_id'
                                       t['intr_NamL'], #'intermediary_last'
                                       t['intr_NamF'], #'intermediary_first'
                                       t['intr_City'], #'intermediary_city'
                                       t['intr_ST'], #'intermediary_state'
                                       t['intr_Zip4'], #'intermediary_zip'
                                       t['intr_Emp'], #'intermediary_emp'
                                       t['intr_Occ'], #'intermediary_occ'
                                       t['filingId']
                                       ])


  campaignTransactions = pd.DataFrame(transactionsList,columns=prod_columns)        
  outputDF = pd.concat([outputDF, campaignTransactions], ignore_index=True)


def get_496:
  countRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/transaction/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':'0',
                            'PageSize':'1',
                            'TransactionType':forms['496'],
                            'ShowSuperceded':'false'})
  formTransactions = countRequest.json()['totalMatchingCount']
  print(formTransactions)

  if formTransactions < 1000:
      requestLoops = 1
  else:
      requestLoops = math.floor(formTransactions/1000)
      
  transactionsList = []

  for i in range(requestLoops):
      page = str(i)
      transactionsRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/transaction/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':page,
                            'PageSize':'1000',
                            'TransactionType':forms['496'],
                            'ShowSuperceded':'false'})
      allTransactions = transactionsRequest.json()['results']
      
      for t in allTransactions:
          if t['sup_Opp_Cd'] == 'S':
              if t['tran_Date'] > lastDeadline:
                fullName = t['cand_NamL']+' '+t['cand_NamF']
                # Create row
                transactionsList.append(['460', #form
                                         t['form_Type'], #schedule
                                         '24-hr Independent Expenditure report', #schedule_description
                                         t['cmte_Id'], #recipient_id
                                         fullName, #recipient_name
                                         dateResponse, #filing_date
                                         t['filingStartDate'], #'report_period_from'
                                         t['filingEndDate'], #'report_period_to'
                                         ' ', #'contributor_code'
                                         t['filerName'], #'contributor_last'
                                         ' ', #'contributor_first'
                                         ' ', #'contributor_city'
                                         ' ', #'contributor_state'
                                         ' ', #'contributor_zip'
                                         ' ', #'contributor_emp'
                                         ' ', #'contributor_occ'
                                         t['tran_Date'], #'contribution_date'
                                         t['tran_Amt1'], #'contribution_amount'
                                         t['tran_Amt2'], #'contribution_annual'
                                         t['tran_Dscr'], #'contribution_desc'
                                         t['filerStateId'], #'contributor_id'
                                         t['intr_NamL'], #'intermediary_last'
                                         t['intr_NamF'], #'intermediary_first'
                                         t['intr_City'], #'intermediary_city'
                                         t['intr_ST'], #'intermediary_state'
                                         t['intr_Zip4'], #'intermediary_zip'
                                         t['intr_Emp'], #'intermediary_emp'
                                         t['intr_Occ'] #'intermediary_occ'
                                         t['filingId']
                                         ])


  campaignTransactions = pd.DataFrame(transactionsList,columns=prod_columns)        
  outputDF = pd.concat([outputDF, campaignTransactions], ignore_index=True)


def get_summary:
  countRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/summary/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':'0',
                            'PageSize':'1',
                            'ShowSuperceded':'false'})
  formTransactions = countRequest.json()['totalMatchingCount']
  print(formTransactions)

  if formTransactions < 1000:
      requestLoops = 1
  else:
      requestLoops = math.floor(formTransactions/1000)
      
  transactionsList = []

  for i in range(requestLoops):
          
      page = str(i)
      transactionsRequest = requests.post('https://netfile.com:443/Connect2/api/public/campaign/export/cal201/summary/year', params=payload,
                    data = {'Aid':'CSD',
                            'Year':'2016',
                            'CurrentPageIndex':page,
                            'PageSize':'1000',
                            'ShowSuperceded':'false'})
      allTransactions = transactionsRequest.json()['results']
      
      for t in allTransactions:
          if t['form_Type'] == 'A' or t['form_Type'] == 'C':
              if t['line_Item'] == '2':
                  if t['form_Type'] == 'A':
                      description = 'Unitemized monetary contributions less than $100'
                      schedule = 'SMRY A'
                  elif t['form_Type'] == 'C':
                      description = 'Unitemized nonmonetary contributions less than $100'
                      schedule = 'SMRY C' 
                  
                  # Create row
                  transactionsList.append(['460', #form
                                       schedule, #schedule
                                       description, #schedule_description
                                       t['filerStateId'], #recipient_id
                                       t['filerName'], #recipient_name
                                       dateResponse, #filing_date
                                       t['filingStartDate'], #'report_period_from'
                                       t['filingEndDate'], #'report_period_to'
                                       ' ', #'contributor_code'
                                       ' ', #'contributor_last'
                                       ' ', #'contributor_first'
                                       ' ', #'contributor_city'
                                       ' ', #'contributor_state'
                                       ' ', #'contributor_zip'
                                       ' ', #'contributor_emp'
                                       ' ', #'contributor_occ'
                                       ' ', #'contribution_date'
                                       t['amount_A'], #'contribution_amount'
                                       t['amount_B'], #'contribution_annual'
                                       ' ', #'contribution_desc'
                                       ' ', #'contributor_id'
                                       ' ', #'intermediary_last'
                                       ' ', #'intermediary_first'
                                       ' ', #'intermediary_city'
                                       ' ', #'intermediary_state'
                                       ' ', #'intermediary_zip'
                                       ' ', #'intermediary_emp'
                                       ' ', #'intermediary_occ'
                                       t['filingId']
                                       ])


  campaignTransactions = pd.DataFrame(transactionsList,columns=prod_columns)        
  outputDF = pd.concat([outputDF, campaignTransactions], ignore_index=True)

def get_filing_dates:
  #filingIdUrl = 'https://netfile.com:443/Connect2/api/public/filing/info/'+filingID
  #dateRequest = requests.get(filingIdUrl, params=payload)
  #dateResponse = dateRequest.json()['filingDate']




