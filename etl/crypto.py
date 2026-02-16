import pandas as pd
import time
from .base_etl import BaseETL
from config.settings import CRYPTO_ASSETS, API_CONFIGS, DATABASE_TABLES, RETRY_CONFIG
from utils.helpers import create_robust_session, safe_api_call


class CryptoETL(BaseETL):
    
    def __init__(self):
        super().__init__("CryptoETL")
        self.session = create_robust_session()
        self.data_frames = []
        
    def extract(self):
        self.logger.info(f"Extracting data for {len(CRYPTO_ASSETS)} cryptocurrencies")
        
        for item in CRYPTO_ASSETS:
            self._extract_asset(item)
            time.sleep(RETRY_CONFIG['wait_between_requests'])
    
    def _extract_asset(self, item: dict):
        if self._try_coingecko(item):
            return
        
        if item['coingecko'] != 'tether':
            self._try_binance(item)
    
    def _try_coingecko(self, item: dict) -> bool:
        try:
            self.logger.info(f"Fetching {item['coingecko']} from CoinGecko")
            url = f"{API_CONFIGS['coingecko_base_url']}/coins/{item['coingecko']}/market_chart"
            params = {'vs_currency': 'usd', 'days': '365', 'interval': 'daily'}
            
            data = safe_api_call(self.session, url, params=params)
            
            if all(k in data for k in ['prices', 'market_caps', 'total_volumes']):
                df = self._process_coingecko_data(data, item)
                self.data_frames.append(df)
                self.logger.success(f"Successfully fetched {item['coingecko']} from CoinGecko")
                return True
                
        except Exception as e:
            self.logger.warning(f"CoinGecko failed for {item['coingecko']}: {e}")
        
        return False
    
    def _try_binance(self, item: dict):
        try:
            self.logger.info(f"Fetching {item['binance']} from Binance")
            url = f"{API_CONFIGS['binance_base_url']}/klines"
            params = {'symbol': item['binance'], 'interval': '1d', 'limit': '365'}
            
            data = safe_api_call(self.session, url, params=params)
            
            if isinstance(data, list) and len(data) > 0:
                df = self._process_binance_data(data, item)
                self.data_frames.append(df)
                self.logger.success(f"Successfully fetched {item['binance']} from Binance")
                
        except Exception as e:
            self.logger.error(f"Binance failed for {item['binance']}: {e}")
    
    def _process_coingecko_data(self, data: dict, item: dict) -> pd.DataFrame:
        df_prices = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
        df_prices['Date'] = pd.to_datetime(df_prices['timestamp'], unit='ms')
        
        df_mc = pd.DataFrame(data['market_caps'], columns=['timestamp', 'market_cap'])
        df_vol = pd.DataFrame(data['total_volumes'], columns=['timestamp', 'volume'])
        
        df_prices['Market_Cap'] = df_mc['market_cap']
        df_prices['Volume'] = df_vol['volume']
        df_prices['Close Price'] = df_prices['price']
        
        moeda = self._normalize_coin_name(item['coingecko'])
        df_prices['Moeda'] = moeda
        
        df_prices['Open Price'] = df_prices['Close Price']
        df_prices['High Price'] = df_prices['Close Price']
        df_prices['Low Price'] = df_prices['Close Price']
        df_prices['Number of Trades'] = 0
        
        return df_prices[['Date', 'Moeda', 'Open Price', 'High Price', 'Low Price', 
                         'Close Price', 'Volume', 'Number of Trades', 'Market_Cap']]
    
    def _process_binance_data(self, data: list, item: dict) -> pd.DataFrame:
        columns = ["Open Time", "Open Price", "High Price", "Low Price", "Close Price", 
                  "Volume", "Close Time", "Quote Asset Volume", "Number of Trades", 
                  "Taker Buy Base", "Taker Buy Quote", "Ignore"]
        df = pd.DataFrame(data, columns=columns)
        
        df['Moeda'] = item['binance'].replace('USDT', '')
        df['Date'] = pd.to_datetime(df['Open Time'], unit='ms')
        
        for col in ["Open Price", "High Price", "Low Price", "Close Price", "Volume"]:
            df[col] = pd.to_numeric(df[col])
        
        df['Market_Cap'] = 0
        
        return df[['Date', 'Moeda', 'Open Price', 'High Price', 'Low Price', 
                  'Close Price', 'Volume', 'Number of Trades', 'Market_Cap']]
    
    def _normalize_coin_name(self, coingecko_id: str) -> str:
        mapping = {
            'tether': 'USDT',
            'binancecoin': 'BNB',
            'ripple': 'XRP'
        }
        return mapping.get(coingecko_id, coingecko_id.upper())
    
    def transform(self):
        if not self.data_frames:
            raise ValueError("No data extracted from any API")
        
        self.logger.info("Consolidating cryptocurrency data")
        self.df = pd.concat(self.data_frames, ignore_index=True)
        
    def load(self):
        self.save_to_database(DATABASE_TABLES['crypto'], if_exists='replace')


if __name__ == "__main__":
    etl = CryptoETL()
    etl.execute()
