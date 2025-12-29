import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine
import urllib
import os
import time

# --- 1. CONFIGURAÇÃO ROBUSTA DE REDE ---
# Isso aqui é o segredo para evitar erros de DNS e Conexão Intermitente
def criar_sessao_robusta():
    session = requests.Session()
    retry = Retry(
        total=5,  # Tenta 5 vezes se der erro
        backoff_factor=2,  # Espera 2s, 4s, 8s entre tentativas
        status_forcelist=[429, 500, 502, 503, 504],  # Erros que merecem retry
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# --- 2. CONFIGURAÇÃO DO BANCO ---
SERVER = os.getenv('DB_SERVER')
DATABASE = os.getenv('DB_NAME')
USERNAME = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASS')

# Validação de Segurança
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

# --- 3. FONTES DE DADOS (LISTA DE MOEDAS) ---
# Mapeamento: [Nome Binance, ID CoinGecko]
moedas_map = [
    {'binance': 'BTCUSDT', 'coingecko': 'bitcoin'},
    {'binance': 'ETHUSDT', 'coingecko': 'ethereum'},
    {'binance': 'SOLUSDT', 'coingecko': 'solana'},
    {'binance': 'ADAUSDT', 'coingecko': 'cardano'}
]

lista_frames = []
session = criar_sessao_robusta()

print(f"--- Iniciando Extração Blindada ---")

for item in moedas_map:
    sucesso = False
    
    # --- TENTATIVA 1: COINGECKO (Padrão Ouro) ---
    try:
        print(f"Tentando {item['coingecko']} via CoinGecko...")
        url = f"https://api.coingecko.com/api/v3/coins/{item['coingecko']}/market_chart?vs_currency=usd&days=365&interval=daily"
        
        # Header falso para parecer um navegador e não ser bloqueado
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = session.get(url, headers=headers, timeout=10)
        data = response.json()
        
        if 'prices' in data:
            df = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
            df['Date'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['Close Price'] = df['price']
            df['Moeda'] = item['binance'].replace('USDT', '')
            
            # Preenchendo colunas extras para manter o padrão
            df['Open Price'] = df['Close Price']
            df['High Price'] = df['Close Price']
            df['Low Price'] = df['Close Price']
            df['Volume'] = 0
            df['Number of Trades'] = 0
            
            lista_frames.append(df[['Date', 'Moeda', 'Open Price', 'High Price', 'Low Price', 'Close Price', 'Volume', 'Number of Trades']])
            print(f"✅ Sucesso via CoinGecko!")
            sucesso = True
            
    except Exception as e:
        print(f"⚠️ CoinGecko falhou para {item['coingecko']}: {e}")

    # --- TENTATIVA 2: BINANCE VISION (Backup Público) ---
    if not sucesso:
        try:
            print(f"Tentando {item['binance']} via Binance Vision...")
            # Endpoint público que costuma ser mais permissivo que o api.binance.com
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
                
                lista_frames.append(df[['Date', 'Moeda', 'Open Price', 'High Price', 'Low Price', 'Close Price', 'Volume', 'Number of Trades']])
                print(f"✅ Sucesso via Binance Vision!")
                sucesso = True
        except Exception as e:
            print(f"❌ Binance Vision também falhou: {e}")

    # Pausa longa obrigatória para não tomar block (CoinGecko é rigorosa)
    print("Aguardando 15 segundos para evitar bloqueio...")
    time.sleep(15)

# --- 4. CONSOLIDAÇÃO E CARGA ---
print(f"--- Consolidando dados ---")

if not lista_frames:
    raise ValueError("ERRO CRÍTICO: Todas as APIs falharam para todas as moedas.")

df_final = pd.concat(lista_frames, ignore_index=True)

print(f"Tentando salvar {len(df_final)} linhas no Azure...")

try:
    df_final.to_sql('tb_binance_historico', con=engine, if_exists='replace', index=False)
    print("✅ SUCESSO ABSOLUTO! Dados carregados no Azure.")
except Exception as e:
    print(f"❌ Erro fatal no SQL: {e}")
    raise e