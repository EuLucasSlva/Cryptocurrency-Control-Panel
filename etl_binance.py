import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine
import urllib
import os
import time

def criar_sessao_robusta():
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

SERVER = os.getenv('DB_SERVER')
DATABASE = os.getenv('DB_NAME')
USERNAME = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASS')

if not all([SERVER, DATABASE, USERNAME, PASSWORD]):
    raise ValueError("❌ ERRO: Secrets não configurados!")

params = urllib.parse.quote_plus(
    f'DRIVER={{ODBC Driver 18 for SQL Server}};'
    f'SERVER={SERVER};'
    f'DATABASE={DATABASE};'
    f'UID={USERNAME};'
    f'PWD={PASSWORD};'
    f'Encrypt=yes;'
    f'TrustServerCertificate=yes;'
    f'Connection Timeout=60;'
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

moedas_map = [
    {'binance': 'BTCUSDT', 'coingecko': 'bitcoin'},
    {'binance': 'ETHUSDT', 'coingecko': 'ethereum'},
    {'binance': 'BNBUSDT', 'coingecko': 'binancecoin'},
    {'binance': 'SOLUSDT', 'coingecko': 'solana'},
    {'binance': 'XRPUSDT', 'coingecko': 'ripple'},
    {'binance': 'DOGEUSDT', 'coingecko': 'dogecoin'},
    {'binance': 'ADAUSDT', 'coingecko': 'cardano'},
    {'binance': 'TRXUSDT', 'coingecko': 'tron'},
    {'binance': 'AVAXUSDT', 'coingecko': 'avalanche-2'},
    {'binance': 'SHIBUSDT', 'coingecko': 'shiba-inu'},
    {'binance': 'DOTUSDT', 'coingecko': 'polkadot'},
    {'binance': 'LINKUSDT', 'coingecko': 'chainlink'},
    {'binance': 'LTCUSDT', 'coingecko': 'litecoin'},
    {'binance': 'NEARUSDT', 'coingecko': 'near'},
    {'binance': 'BTCUSDT', 'coingecko': 'tether'} 
]

lista_frames = []
session = criar_sessao_robusta()

print(f"--- Iniciando Extração Top 15 com Market Cap ---")

for item in moedas_map:
    sucesso = False
    
    try:
        print(f"Tentando {item['coingecko']} via CoinGecko...")
        url = f"https://api.coingecko.com/api/v3/coins/{item['coingecko']}/market_chart?vs_currency=usd&days=365&interval=daily"
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = session.get(url, headers=headers, timeout=10)
        data = response.json()
        
        if 'prices' in data and 'market_caps' in data:
            df_prices = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
            df_prices['Date'] = pd.to_datetime(df_prices['timestamp'], unit='ms')
            
            df_mc = pd.DataFrame(data['market_caps'], columns=['timestamp', 'market_cap'])
            
            df_prices['Market_Cap'] = df_mc['market_cap']
            
            df_prices['Close Price'] = df_prices['price']
            df_prices['Moeda'] = item['coingecko'].upper()
            
            if item['coingecko'] == 'tether':
                 df_prices['Moeda'] = 'USDT'
            elif item['coingecko'] == 'binancecoin':
                 df_prices['Moeda'] = 'BNB'
            elif item['coingecko'] == 'ripple':
                 df_prices['Moeda'] = 'XRP'

            df_prices['Open Price'] = df_prices['Close Price']
            df_prices['High Price'] = df_prices['Close Price']
            df_prices['Low Price'] = df_prices['Close Price']
            df_prices['Volume'] = 0
            df_prices['Number of Trades'] = 0

            lista_frames.append(df_prices[['Date', 'Moeda', 'Open Price', 'High Price', 'Low Price', 'Close Price', 'Volume', 'Number of Trades', 'Market_Cap']])
            print(f"✅ Sucesso via CoinGecko (com Market Cap)!")
            sucesso = True
            
    except Exception as e:
        print(f"⚠️ CoinGecko falhou para {item['coingecko']}: {e}")

    # TENTATIVA 2: BINANCE VISION (Backup - Sem Market Cap)
    if not sucesso and item['coingecko'] != 'tether': # Pula Tether no fallback da Binance pq o par USDTUSDT é estranho
        try:
            print(f"Tentando {item['binance']} via Binance Vision...")
            url = "https://data-api.binance.vision/api/v3/klines"
            params_api = {'symbol': item['binance'], 'interval': '1d', 'limit': '365'}
            
            response = session.get(url, params=params_api, timeout=10)
            data = response.json()
            
            if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
                colunas = ["Open Time", "Open Price", "High Price", "Low Price", "Close Price", "Volume", "Close Time", "Quote Asset Volume", "Number of Trades", "Taker Buy Base", "Taker Buy Quote", "Ignore"]
                df = pd.DataFrame(data, columns=colunas)
                
                df['Moeda'] = item['binance'].replace('USDT', '')
                df['Date'] = pd.to_datetime(df['Open Time'], unit='ms')
                
                cols_num = ["Open Price", "High Price", "Low Price", "Close Price", "Volume"]
                for c in cols_num: df[c] = pd.to_numeric(df[c])
                
                df['Market_Cap'] = 0 
                
                lista_frames.append(df[['Date', 'Moeda', 'Open Price', 'High Price', 'Low Price', 'Close Price', 'Volume', 'Number of Trades', 'Market_Cap']])
                print(f"✅ Sucesso via Binance Vision (Sem Market Cap)!")
                sucesso = True
        except Exception as e:
            print(f"❌ Binance Vision também falhou: {e}")

    print("Aguardando 12 segundos...")
    time.sleep(12)

print(f"--- Consolidando dados ---")

if not lista_frames:
    raise ValueError("ERRO CRÍTICO: Todas as APIs falharam.")

df_final = pd.concat(lista_frames, ignore_index=True)

print(f"Tentando salvar {len(df_final)} linhas no Azure...")

try:
    df_final.to_sql('tb_binance_historico', con=engine, if_exists='replace', index=False)
    print("✅ SUCESSO ABSOLUTO! Dados carregados com Market Cap.")
except Exception as e:
    print(f"❌ Erro fatal no SQL: {e}")
    raise e