import gspread
import google.auth
import json
from google.oauth2 import service_account

class GoogleSheetsManager:
    def __init__(self, spreadsheet_name, spreadsheet_id=None, worksheet_name=None):
        self.scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        self.spreadsheet_name = spreadsheet_name
        self.client = self._authenticate()
        
        if spreadsheet_id:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
        else:
            spreadsheet = self.client.open(spreadsheet_name)
            
        if worksheet_name:
            try:
                self.sheet = spreadsheet.worksheet(worksheet_name)
            except gspread.exceptions.WorksheetNotFound:
                self.sheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
        else:
            self.sheet = spreadsheet.get_worksheet(0)

    def _authenticate(self):
        """
        Authenticates using a service account key stored in the GOOGLE_SHEETS_SA_KEY
        Airflow Variable (JSON string). Falls back to Application Default Credentials.
        """
        try:
            from airflow.sdk import Variable
            sa_key_json = Variable.get("GOOGLE_SHEETS_SA_KEY")
            sa_info = json.loads(sa_key_json)
            credentials = service_account.Credentials.from_service_account_info(
                sa_info, scopes=self.scope
            )
            return gspread.authorize(credentials)
        except Exception:
            pass

        # Fallback: ADC (works locally if gcloud scopes include Sheets)
        try:
            credentials, _ = google.auth.default(scopes=self.scope)
            return gspread.authorize(credentials)
        except Exception as e:
            print(f"Failed to authenticate with Google Sheets: {e}")
            raise

    def append_leads(self, leads):
        """
        Appends a list of dictionaries to the Google Sheet.
        """
        if not leads:
            print("No leads to append.")
            return

        # Dynamically create headers if the sheet is empty
        header = self.sheet.row_values(1)
        if not header:
            header = list(leads[0].keys())
            self.sheet.insert_row(header, 1)
        
        # Flatten the data to match the header columns
        rows = []
        for lead in leads:
            row = [str(lead.get(col, "")) for col in header]
            rows.append(row)
            
        self.sheet.append_rows(rows)
        print(f"✅ Successfully appended {len(leads)} leads to {self.spreadsheet_name}")

