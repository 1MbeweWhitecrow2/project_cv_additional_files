import requests
import pandas as pd
import time
from tqdm import tqdm

# --- Config ---
ALPHAVANTAGE_API_KEY = "Place_Your_Key_Here"  # Change to your API Key
BASE_URL = "https://www.alphavantage.co/query"
TIME_SERIES_CSV = "sp500_TimeSeries_adjusted.csv"
REQUEST_INTERVAL = 2  # 2 sec delay between requests, to be in line with Alpha Vantage limits

# Get company names and sectors from Wikipedia
sp500_df = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
sp500_df = sp500_df[['Symbol', 'Security', 'GICS Sector']]

# Change ticker format to be in line with Alpha Vantage API
sp500_df = sp500_df.rename(columns={'Symbol': 'ticker', 'Security': 'name', 'GICS Sector': 'sector'})
sp500_df['ticker'] = sp500_df['ticker'].str.replace('.', '-', regex=False)

all_timeseries_data = []

for _, row in tqdm(sp500_df.iterrows(), total=sp500_df.shape[0], desc="Getting data"):
    ticker = row['ticker']
    name = row['name']
    sector = row['sector']

    params_ts = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": ticker,
        "outputsize": "full",
        "apikey": ALPHAVANTAGE_API_KEY
    }

    response_ts = requests.get(BASE_URL, params=params_ts)
    data_ts = response_ts.json().get("Time Series (Daily)", {})

    if not data_ts:
        print(f"[Warning] No data for {ticker}")
    else:
        adjustment_factor = 1.0
        for date, values in sorted(data_ts.items(), reverse=True):
            split_coefficient = float(values.get("8. split coefficient", 1))
            dividend_amount = float(values.get("7. dividend amount", 0))

            adjustment_factor /= split_coefficient * (1 + dividend_amount / float(values["5. adjusted close"]))

            adjusted_open = float(values["1. open"]) * adjustment_factor
            adjusted_high = float(values["2. high"]) * adjustment_factor
            adjusted_low = float(values["3. low"]) * adjustment_factor
            adjusted_close = float(values["5. adjusted close"])

            all_timeseries_data.append({
                "ticker": ticker,
                "name": name,
                "sector": sector,
                "date": date,
                "adjusted_open": adjusted_open,
                "adjusted_high": adjusted_high,
                "adjusted_low": adjusted_low,
                "adjusted_close": adjusted_close,
                "volume": int(values["6. volume"])
            })

    time.sleep(REQUEST_INTERVAL)

# Zapisz dane do pliku CSV
pd.DataFrame(all_timeseries_data).to_csv(TIME_SERIES_CSV, index=False)
print(f"✅ Data written to {TIME_SERIES_CSV}")
