import pandas as pd
import requests
from .base_etl import BaseETL
from config.settings import API_CONFIGS, DATABASE_TABLES


class CurrencyETL(BaseETL):
    
    def __init__(self):
        super().__init__("CurrencyETL")
        self.exchange_rate = None
        
    def extract(self):
        self.logger.info("Fetching USD/BRL exchange rate")
        self.exchange_rate = self._fetch_exchange_rate()
        
    def _fetch_exchange_rate(self) -> dict:
        try:
            response = requests.get(API_CONFIGS['awesomeapi_url'])
            if response.status_code == 200:
                data = response.json()
                self.logger.info("Successfully fetched from AwesomeAPI")
                return {"bid": data['USDBRL']['bid'], "source": "AwesomeAPI"}
        except Exception as e:
            self.logger.warning(f"AwesomeAPI failed: {e}. Trying fallback...")
        
        try:
            response = requests.get(API_CONFIGS['fallback_currency_url'])
            data = response.json()
            self.logger.info("Successfully fetched from FallbackAPI")
            return {"bid": data['usd']['brl'], "source": "FallbackAPI"}
        except Exception as e:
            raise Exception(f"All currency APIs failed: {e}")
    
    def transform(self):
        self.logger.info("Transforming currency data")
        self.df = pd.DataFrame([{
            "moeda": "USD",
            "bid": float(self.exchange_rate['bid']),
            "data_consulta": pd.Timestamp.now()
        }])
        
    def load(self):
        self.db_config.test_connection()
        self.save_to_database(DATABASE_TABLES['currency'], if_exists='replace')


if __name__ == "__main__":
    etl = CurrencyETL()
    etl.execute()
