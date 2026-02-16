import pandas as pd
import yfinance as yf
from .base_etl import BaseETL
from config.settings import BRAZILIAN_STOCKS, DATABASE_TABLES


class BrazilianStocksETL(BaseETL):
    
    def __init__(self):
        super().__init__("BrazilianStocksETL")
        
    def extract(self):
        self.logger.info(f"Downloading data for {len(BRAZILIAN_STOCKS)} Brazilian stocks")
        self.raw_data = yf.download(
            BRAZILIAN_STOCKS, 
            period="2y"
        )[["Open", "High", "Low", "Close", "Volume"]]
        
    def transform(self):
        self.logger.info("Transforming Brazilian stocks data")
        
        df_stack = self.raw_data.stack(level=1).reset_index()
        df_stack.columns = ['Date', 'Ticker', 'Close', 'High', 'Low', 'Open', 'Volume']
        
        df_stack['Ticker'] = df_stack['Ticker'].str.replace('.SA', '', regex=False)
        
        numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        df_stack[numeric_cols] = df_stack[numeric_cols].round(2)
        
        df_stack.dropna(subset=['Close'], inplace=True)
        
        self.df = df_stack
        self.logger.info(f"Transformed {len(self.df)} rows")
        
    def load(self):
        self.save_to_database(DATABASE_TABLES['brazilian_stocks'], if_exists='replace')


if __name__ == "__main__":
    etl = BrazilianStocksETL()
    etl.execute()
