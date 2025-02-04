from sqlalchemy import create_engine

# Create a database connection

engine = create_engine("postgresql://postgres:password@localhost:5432/stocks_db")  

# Load data into the database

data.to_sql("stocks", engine, if_exists="replace", index=False, method="multi")
print("Data successfully loaded into the database!")


