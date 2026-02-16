import pandas as pd
import yfinance as yf
from .base_etl import BaseETL
from config.settings import NASDAQ_STOCKS, DATABASE_TABLES


class NasdaqStocksETL(BaseETL):
    
    def __init__(self):
        super().__init__("NasdaqStocksETL")
        
    def extract(self):
        self.logger.info(f"Downloading data for {len(NASDAQ_STOCKS)} NASDAQ stocks")
        self.raw_data = yf.download(
            NASDAQ_STOCKS, 
            period="2y"
        )[["Open", "High", "Low", "Close", "Volume"]]
        
    def transform(self):
        self.logger.info("Transforming NASDAQ stocks data")
        
        df_stack = self.raw_data.stack(level=1).reset_index()
        df_stack.columns = ['Date', 'Ticker', 'Close', 'High', 'Low', 'Open', 'Volume']
        
        numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        df_stack[numeric_cols] = df_stack[numeric_cols].round(2)
        
        df_stack.dropna(subset=['Close'], inplace=True)
        df_stack['Mercado'] = 'NASDAQ'
        
        self.df = df_stack
        self.logger.info(f"Transformed {len(self.df)} rows")
        
    def load(self):
        self.save_to_database(DATABASE_TABLES['nasdaq_stocks'], if_exists='replace')


if __name__ == "__main__":
    etl = NasdaqStocksETL()
    etl.execute()
