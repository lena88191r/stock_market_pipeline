from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import yfinance as yf
import pandas as pd

def extract_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Extract historical stock data for a given ticker symbol and date range.

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


def transform_data(stock_data):
   """ Cleans and transforms the data extracted to make it ready for analysis.
	
	Params:
	
	stock_data: DataFrame"""

	# Check for Missing Values: Use Pandas to check if any columns contain null values.
	print(stock_data.isnull().sum())  # Check for missing values
	
	# Handle Missing Data: filling with the previous value:
	stock_data.fillna(method='ffill', inplace=True)
	
	# Normalize column names for database compatibility (lowercase, snake_case):
	stock_data.columns = stock_data.columns.str.replace (" ", "_").str.lower()
	
	# Calculating moving averages and volatility indicators to enhance the dataset:
	# Calculate Moving Averages:
	stock_data['moving_avg_50'] = stock_data['Close'].rolling(window=50).mean()
	stock_data['moving_avg_200'] = stock_data['Close'].rolling(window=200).mean()
	
	# Daily Returns:
	stock_data['daily_return'] = stock_data['Close'].pct_change()
	
	# Volatility:
	stock_data['volatility'] = stock_data['Close'].rolling(window=30).std()
	
	return stock_data

def load_data(stock_data):
    # Load to database
    # Create a database connection
    engine = create_engine("postgresql://postgres:password@localhost:5432/stocks_db")  
    
    # Load data into the database
    stock_data.to_sql("stocks", engine, if_exists="replace", index=False, method="multi")
    print("Data successfully loaded into the database!")
    pass

# Define the DAG
with DAG(
    'stock_etl_pipeline',
    default_args={'start_date': datetime(2024, 1, 1)},
    schedule_interval='@daily',
) as dag:
    extract = PythonOperator(task_id='extract', python_callable=extract_data)
    transform = PythonOperator(task_id='transform', python_callable=transform_data)
    load = PythonOperator(task_id='load', python_callable=load_data)

    extract >> transform >> load
