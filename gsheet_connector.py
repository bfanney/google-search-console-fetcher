import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

class g:

    def __init__(self, docid):
        self.docid = docid
        
        print ("Logging into Google.")
        scope = ['https://spreadsheets.google.com/feeds']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('search_client_gsheet_client_secrets.json', scope)
        client = gspread.authorize(credentials)
        self.sh = client.open_by_key(self.docid)

    def setGoogleSheet(self, data, tab):
        worksheet = self.sh.worksheet(tab)
        print ("Uploading Google Sheet.")
        set_with_dataframe(worksheet, data)

    def blankGoogleSheet(self, tab):
        worksheet = self.sh.worksheet(tab)
        print ("Blanking Google Sheet.")
        sheet = worksheet.get_all_values()

        headers = sheet.pop(0)
        sheet = pd.DataFrame(sheet, columns=headers)

        length = len(sheet.index)+1

        cells = "A1:Y" + str(length)

        cell_list = worksheet.range(cells)
        for cell in cell_list:
            cell.value = ""

        worksheet.update_cells(cell_list)

    def getGoogleSheet(self, tab):
        worksheet = sh.worksheet(tab)
        print ("Downloading Google Sheet.")
        #Get Form Responses tab as a list of lists
        sheet = worksheet.get_all_values()

        #Convert sheet to dataframe
        headers = sheet.pop(0)
        sheet = pd.DataFrame(sheet, columns=headers)

        return sheet

    def getNewRows(self, new, old):
        print ("Getting new rows.")
        return new[~new.index.isin(old.index)]

    def getSheetNames(self):
        return self.sh.worksheets()

    def setNewSheet(self, tab):
        self.sh.add_worksheet(tab,rows="100", cols="20")
        