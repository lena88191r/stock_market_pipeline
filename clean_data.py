import pandas as pd

def clean_data (stock_data):
	""" Cleans and transforms the data extracted to make it ready for analysis.
	
	Params:
	
	data: DataFrame"""

	# Check for Missing Values: Use Pandas to check if any columns contain null values.
	
	print(stock_data.isnull().sum())  # Check for missing values
	
	# Handle Missing Data: filling with the previous value:
	
	stock_data.fillna(method='ffill', inplace=True)
	
	# Normalize column names for database compatibility (lowercase, snake_case):
	
	stock_data.columns = stock_data.columns.str.replace (" ", "_").str.lower()
	
	# Calculating moving averages and volatility indicators to enhance the dataset:
	
	# Calculate Moving Averages:

	stock_data['moving_avg_50'] = stock_data['close'].rolling(window=50).mean()
	stock_data['moving_avg_200'] = stock_data['close'].rolling(window=200).mean()
	
	# Daily Returns:

	stock_data['daily_return'] = stock_data['close'].pct_change()
	
	# Volatility:

	stock_data['volatility'] = stock_data['close'].rolling(window=30).std()

    stock_data = data
	
	return data
