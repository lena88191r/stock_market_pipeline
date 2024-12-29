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
 
 
# Main script EXTRACT ONLY
if __name__ == "__main__":
    

   
# Cleaning script

def clean_data (data):
	""" Cleans and transforms the data extracted to make it ready for analysis.
	
	Params:
	
	data: DataFrame"""

	# Check for Missing Values: Use Pandas to check if any columns contain null values.
	
	print(data.isnull().sum())  # Check for missing values
	
	# Handle Missing Data: filling with the previous value:
	
	data.fillna(method='ffill', inplace=True)
	
	# Normalize column names for database compatibility (lowercase, snake_case):
	
	data.columns = data.columns.str.replace (" ", "_").str.lower()
	
	# Calculating moving averages and volatility indicators to enhance the dataset:
	
	# Calculate Moving Averages:

	data['moving_avg_50'] = data['Close'].rolling(window=50).mean()
	data['moving_avg_200'] = data['Close'].rolling(window=200).mean()
	
	# Daily Returns:

	data['daily_return'] = data['Close'].pct_change()
	
	# Volatility:

	data['volatility'] = data['Close'].rolling(window=30).std()
	
	return data
	

from sqlalchemy import create_engine

# Create a database connection

engine = create_engine("postgresql://postgres:password@localhost:5432/stocks_db")  

# Load data into the database

data.to_sql("stocks", engine, if_exists="replace", index=False, method="multi")
print("Data successfully loaded into the database!")


