import pandas as pd
import requests
from sqlalchemy import create_engine
import urllib
import time 
import os

# --- 1. CONFIGURAÇÃO E VALIDAÇÃO DE AMBIENTE ---
SERVER = os.getenv('DB_SERVER')
DATABASE = os.getenv('DB_NAME')
USERNAME = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASS')

print(f"--- Iniciando Extração Binance ---")
print(f"Servidor: {SERVER} | Banco: {DATABASE}")

if not all([SERVER, DATABASE, USERNAME, PASSWORD]):
    raise ValueError("ERRO: Segredos do banco de dados não configurados no GitHub!")

params = urllib.parse.quote_plus(
    f'DRIVER={{ODBC Driver 18 for SQL Server}};'
    f'SERVER={SERVER};'
    f'DATABASE={DATABASE};'
    f'UID={USERNAME};'
    f'PWD={PASSWORD};'
    f'TrustServerCertificate=yes;'
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# --- 2. EXTRAÇÃO MULTI-MOEDAS ---

moedas = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'ADAUSDT']
url = "https://api.binance.com/api/v3/klines"
lista_frames = []

for par in moedas:
    params_api = {
        'symbol': par,
        'interval': '1d',
        'limit': '1000'
    }

    response = requests.get(url, params=params_api)
    data = response.json()
    
    # Log de volume de dados
    print(f"Moeda: {par} | Linhas recebidas da API: {len(data)}")

    colunas = [
        "Open Time", "Open Price", "High Price", "Low Price", "Close Price",
        "Volume", "Close Time", "Quote Asset Volume", "Number of Trades",
        "Taker Buy Base", "Taker Buy Quote", "Ignore"
    ]
    df_temp = pd.DataFrame(data, columns=colunas)
    df_temp['Moeda'] = par.replace('USDT', '')
    lista_frames.append(df_temp)
    
    time.sleep(1)

# --- 3. CONSOLIDAÇÃO ---
df_total = pd.concat(lista_frames, ignore_index=True)

cols_to_numeric = ["Open Price", "High Price", "Low Price", "Close Price", "Volume"]
for col in cols_to_numeric:
    df_total[col] = pd.to_numeric(df_total[col])

df_total['Date'] = pd.to_datetime(df_total['Open Time'], unit='ms')
df_final = df_total[['Date', 'Moeda', 'Open Price', 'High Price', 'Low Price', 'Close Price', 'Volume', 'Number of Trades']]

# --- 4. CARGA COM ERRO EXPLÍCITO ---
print(f"Tentando salvar {len(df_final)} linhas totais no Azure...")

if len(df_final) == 0:
    raise ValueError("ERRO: O DataFrame final está vazio. Nada será enviado ao banco.")

try:
    df_final.to_sql('tb_binance_historico', con=engine, if_exists='replace', index=False)
    print("✅ Sucesso! Todas as moedas foram carregadas no Azure.")
except Exception as e:
    print(f"❌ Erro fatal no SQL (Binance): {e}")
    raise e # Faz o GitHub Actions falhar se o banco recusar os dados