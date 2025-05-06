import yfinance as yf
import pandas as pd
import time
import random
import requests_cache
from tqdm import tqdm

# --- CONFIGURATION ---
CSV_FILENAME = "sp500_financials.csv"
RATE_LIMIT_PERIOD = 2  # Prevents blocking

# --- LOAD S&P 500 STOCKS ---
sp500_df = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
sp500_tickers = sp500_df['Symbol'].tolist()
sp500_tickers = [t.replace(".", "-") for t in sp500_tickers]  # Fix format

# --- SETUP CACHING ---
cache_session = requests_cache.CachedSession("yfinance_cache", expire_after=86400)

# Define Financial Columns
columns = [
    "ticker", "eps", "dividend_yield", "net_income", "normalized_income",
    "total_expenses", "repurchase_stock", "net_debt", "total_assets", "total_revenue"
]

all_data = []

# --- FETCH DATA ---
for ticker in tqdm(sp500_tickers[:10], desc="Fetching Financials"):
    try:
        stock = yf.Ticker(ticker, session=cache_session)  # Cached requests

        # Fetch financial data
        financials = stock.financials.fillna(method="bfill").fillna(method="ffill")
        balance_sheet = stock.balance_sheet.fillna(method="bfill").fillna(method="ffill")
        cash_flow = stock.cash_flow.fillna(method="bfill").fillna(method="ffill")

        # Extract latest available values, fallback to 0 if missing
        def get_latest(df, key):
            return df.loc[key].iloc[-1] if key in df.index else 0

        eps = get_latest(financials, "Diluted EPS")
        dividend_yield = get_latest(financials, "Dividends Paid")
        net_income = get_latest(financials, "Net Income")
        normalized_income = get_latest(financials, "Normalized Income")
        total_expenses = get_latest(financials, "Total Expenses")
        repurchase_stock = get_latest(cash_flow, "Repurchase of Capital Stock")
        net_debt = get_latest(balance_sheet, "Net Debt")
        total_assets = get_latest(balance_sheet, "Total Assets")
        total_revenue = get_latest(financials, "Total Revenue")

        # Append Data
        all_data.append({
            "ticker": ticker,
            "eps": eps,
            "dividend_yield": dividend_yield,
            "net_income": net_income,
            "normalized_income": normalized_income,
            "total_expenses": total_expenses,
            "repurchase_stock": repurchase_stock,
            "net_debt": net_debt,
            "total_assets": total_assets,
            "total_revenue": total_revenue
        })

        print(f"[INFO] {ticker} Financials Retrieved")

        # --- RATE LIMIT ---
        time.sleep(random.uniform(RATE_LIMIT_PERIOD, RATE_LIMIT_PERIOD + 1))

    except Exception as e:
        print(f"[ERROR] Could not fetch financial data for {ticker}: {e}")

# --- SAVE TO CSV ---
df = pd.DataFrame(all_data)
df.to_csv(CSV_FILENAME, index=False)

print(f"\n✅ Financial data saved to {CSV_FILENAME}")
