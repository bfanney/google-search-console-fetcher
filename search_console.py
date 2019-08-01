import httplib2
import pandas as pd
import os
from apiclient import errors
from apiclient.discovery import build
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from oauth2client.client import OAuth2WebServerFlow
import datetime
import gsheet_connector as gsheet
import csv
from datetime import date, datetime, timedelta
import os.path
from os import path

def get_auth():
    scope = 'https://www.googleapis.com/auth/webmasters.readonly'
    creds = 'client_secrets.json'
    client_secrets = os.path.join(os.path.dirname(creds),creds)
    flow = client.flow_from_clientsecrets(client_secrets,scope=scope,message=tools.message_if_missing(client_secrets))

    #Store credentials so I don't have to log in everytime
    storage = file.Storage('creds.dat')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage)    

    #Create an httplib2.Http object and authorize it with credentials
    http = httplib2.Http()
    http = credentials.authorize(http)

    #Create service object, then return it 
    service = build('webmasters', 'v3', http=http)
    return service

def execute_request(service, property_uri, request):
    return service.searchanalytics().query(siteUrl=property_uri, body=request).execute()

def upload_sheet(table,title):
    sheet = gsheet.g("1rJQcN77v1RfBmDFpqh11kINBV4EZIK98KWCpBAO4ups")
    
    #Check if there's a tab that matches the title. If not, make a new one. Each tab will be overwritten each program run.
    tabs = sheet.getSheetNames()
    match=False
    for tab in tabs:
        if tab.title == title:
            match=True
    if match==False:
        sheet.setNewSheet(title)
    
    sheet.setGoogleSheet(table,title)

def save_table(response, title, key_names, start_date):
    print ("Month: " + start_date)
    print ("Saving table")

    #Do some error checking
    if 'rows' not in response:
        print('Empty response')
        return

    filename = title + ".csv"

    #Convert to dataframe
    rows = response['rows']
    table = pd.DataFrame.from_dict(rows)
    table["month"]=start_date
    
    #Split keys into their own columns
    table[key_names] = pd.DataFrame(table['keys'].tolist(), index=table.index)

    #Rearrange order, drop original keys column
    table = table[['month'] + key_names + ['clicks','impressions','ctr','position']]

    #if a file already exists, load the old values and add the new table below it
    if path.exists(filename):
        old_table = pd.read_csv(filename)
        table = pd.concat([table,old_table],join_axes=[table.columns])

    #Save as CSV
    table.to_csv(filename)

    print ("File saved")
    print ("-------------------------")

    return table

"""
Program use
Use this script to download responses from the Google Search Console API and save them to a Google Sheet and CSVs.

OAuth is a pain
To use this program, you'll need two keys:
1.) Go to https://console.developers.google.com/apis/dashboard
2.) Create a project. Give it access to the Search Console and Google Spreadsheet APIs.
3.) Create an OAuth client ID credential for Search Console. Download it as a JSON and save it as client_secrets.json. When you login, you'll
    login via a web browser and get credentials good for 30 days. They'll be stored as creds.dat. This is problematic for a webserver, since
    this requires human interaction, but I get permissions errors when I try to use a service account key.
4.) Create a Service account key for for Google Spreadsheet. Download it as a JSON and save it as search_client_gsheet_client_secrets.json.
    This should work indefinitely. You'll need to share the spreadsheet with the email associated with your credential.
"""

#Change these dates to change the fetching range
dates = pd.date_range('2018-03-01','2019-06-01', freq='MS').strftime("%Y-%m-%d").tolist()
url = "https://www.redhat.com"
service = get_auth()

#This go thing skips the first value in the list of dates so there's both a start date and an end date for the API to process
go=False
for date in dates:
    if go==True:
        
        start_date = prev_date
        end_date = date
        
        print ("Running top queries request...")
        request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['query','page'],
            'rowLimit': 25000
        }
        response = execute_request(service, url, request)
        table = save_table(response, 'Top Queries', request['dimensions'], start_date)
    
    prev_date = date
    go=True

#This uploads the full table as a sheet
#upload_sheet(table,"Top Queries")

print ("Done.")

#Go back as far as possible, then 3 days after end of month create a new sheet