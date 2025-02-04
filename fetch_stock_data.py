import yfinance as yf
import pandas as pd

# Function to fetch stock data
def fetch_stock_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch historical stock data for a given ticker symbol and date range.

    Parameters:
        ticker (str): Stock ticker symbol (e.g., 'AAPL' for Apple).
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: DataFrame containing stock data (Open, High, Low, Close, Volume).
    """
    stock_data = yf.download(ticker, start=start_date, end=end_date)
    stock_data.reset_index(inplace=True)  # Reset index to include 'Date' as a column

    return stock_data
