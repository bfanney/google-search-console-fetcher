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
import csv
from datetime import date, datetime, timedelta
import os.path
from os import path
from dateutil import parser

class Console:

    def __init__(self,url,creds):
        scope = 'https://www.googleapis.com/auth/webmasters.readonly'
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
        self.service = build('webmasters', 'v3', http=http)
        self.url=url

    def execute_request(self, request):
        return self.service.searchanalytics().query(siteUrl=self.url, body=request).execute()

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
        print ("The CSV file already exists. The script will add the new values to the top.")
        old_table = pd.read_csv(filename)
        table = pd.concat([table,old_table],join_axes=[table.columns])

    #Save as CSV
    table.to_csv(filename)

    print ("File saved")
    print ("-------------------------")

    return table

"""
Program use:
Use this script to download responses from the Google Search Console API and save them to a Google Sheet and CSVs.

Python version:
Please ensure you use Python 3. You'll get unicode errors otherwise.

OAuth is a pain:
To use this program, you'll need two keys:
1.) Go to https://console.developers.google.com/apis/dashboard
2.) Create a project. Give it access to the Search Console and Google Spreadsheet APIs.
3.) Create an OAuth client ID credential for Search Console. Download it as a JSON and save it as client_secrets.json. When you login, you'll
    login via a web browser and get credentials good for 30 days. They'll be stored as creds.dat. This is problematic for a webserver, since
    this requires human interaction, but I get permissions errors when I try to use a service account key.
4.) Create a Service account key for for Google Spreadsheet. Download it as a JSON and save it as search_client_gsheet_client_secrets.json.
    This should work indefinitely. You'll need to share the spreadsheet with the email associated with your credential.
"""

#Get first day of current month and previous month
end_date = datetime.today().replace(day=1)
start_date = (end_date - timedelta(days=1)).replace(day=1)

#Format datetime objects as strings
start_date = start_date.strftime("%Y-%m-%d")
end_date = end_date.strftime("%Y-%m-%d")

#Uncomment and change these dates to change the fetching range. The script will grab values between these dates one month at a time. Otherwise, the program will just grab last month's data.
start_date = '2019-05-01'
end_date = '2019-06-01'

print ("Start date: " + start_date)
print ("End date: " + end_date)

#If today's date is less than 3 and you're request a report including this month, don't execute because the data isn't available yet
#Yes, this line of code is an abomination
if int(datetime.today().strftime("%d"))<3 and parser.parse(end_date).strftime("%Y-%m-%d")==datetime.today().replace(day=1).strftime("%Y-%m-%d"):
    print ("Error: Data not available yet. Please wait until the 4th day of this month for last month's data to become available.")

#Also check if we're asking for future data
elif parser.parse(end_date)>datetime.now():
    print ("Error: I can't see into the future.")

else:
    dates = pd.date_range(start_date,end_date, freq='MS').strftime("%Y-%m-%d").tolist()
    print (dates)

    #Create Console object
    url = "https://www.redhat.com"
    creds = "client_secrets.json"
    service = Console(url,creds)

    #This go variable skips the first value in the list of dates so there's both a start date and an end date for the API to process
    go=False
    for date in dates:
        if go==True:
                    
            print ("Running top queries request...")
            request = {
                'startDate': prev_date,
                'endDate': date,
                'dimensions': ['query','page'],
                'rowLimit': 25000
            }
            response = service.execute_request(request)
            table = save_table(response, 'Top Queries', request['dimensions'], prev_date)
        
        prev_date = date
        go=True

    print ("Done.")