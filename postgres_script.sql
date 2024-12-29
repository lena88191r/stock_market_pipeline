-- Create PostgreSQL database:

CREATE DATABASE stocks_db;

-- Create the table for the data:

CREATE TABLE stocks (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10),
    date DATE NOT NULL,
    open NUMERIC(10, 2),
    high NUMERIC(10, 2),
    low NUMERIC(10, 2),
    close NUMERIC(10, 2),
    adjusted_close NUMERIC(10, 2),
    volume BIGINT,
    moving_avg_50 NUMERIC(10, 2),
    moving_avg_200 NUMERIC(10, 2),
    daily_return NUMERIC(10, 4),
    volatility NUMERIC(10, 4)
);

--Create index:

CREATE INDEX idx_ticker_date ON stocks (ticker, date);
