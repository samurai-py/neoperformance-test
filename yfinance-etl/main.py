from datetime import datetime, timedelta

import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def download_stock_data(companies):
    dfs = []
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

    for company in companies:
        data = yf.download(company, start=start_date, end=end_date)
        data['Company'] = company
        data['Date'] = data.index
        data.reset_index(drop=True, inplace=True)
        dfs.append(data)

    final_df = pd.concat(dfs, ignore_index=True)
    final_df = final_df[['Date', 'Company', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']].sort_values(by=['Date'])
    final_df['Date'] = final_df['Date'].astype(str)

    return final_df

def authenticate_google_sheets(credentials_path, sheet_name):
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file",
             "https://www.googleapis.com/auth/drive"]

    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name).sheet1

    return sheet

def insert_data_into_sheet(sheet, data):
    data_to_insert = [data.columns.values.tolist()] + data.values.tolist()
    sheet.insert_rows(data_to_insert, value_input_option='RAW')

def main():
    companies = ['META', 'AMZN', 'AAPL', 'NFLX', 'GOOGL']
    sheet_name = "yfinance-data"

    final_df = download_stock_data(companies)
    sheet = authenticate_google_sheets(credentials_path, sheet_name)
    insert_data_into_sheet(sheet, final_df)

    print(final_df)

if __name__ == "__main__":
    main()
