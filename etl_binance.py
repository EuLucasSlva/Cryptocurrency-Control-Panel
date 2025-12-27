import pandas as pd
import requests
from sqlalchemy import create_engine
import urllib
import time # Importante para não ser bloqueado pela API
import os

# --- 1. CONFIGURAÇÃO DO BANCO (Igual ao anterior) ---
SERVER = os.getenv('DB_SERVER')
DATABASE = os.getenv('DB_NAME')
USERNAME = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASS')

params = urllib.parse.quote_plus(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={SERVER};'
    f'DATABASE={DATABASE};'
    f'UID={USERNAME};'
    f'PWD={PASSWORD}'
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# --- 2. EXTRAÇÃO E TRANSFORMAÇÃO MULTI-MOEDAS ---

# Definimos a lista de moedas que queremos
moedas = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'ADAUSDT']
url = "https://api.binance.com/api/v3/klines"

# Criamos uma lista vazia para guardar os DataFrames de cada moeda
lista_frames = []

print("Iniciando extração múltipla...")

for par in moedas:
    print(f"Buscando dados de: {par}")
    
    params_api = {
        'symbol': par,
        'interval': '1d',
        'limit': '1000'
    }

    # Faz o pedido para a API
    response = requests.get(url, params=params_api)
    data = response.json()

    # Transforma em DataFrame temporário
    colunas = [
        "Open Time", "Open Price", "High Price", "Low Price", "Close Price",
        "Volume", "Close Time", "Quote Asset Volume", "Number of Trades",
        "Taker Buy Base", "Taker Buy Quote", "Ignore"
    ]
    df_temp = pd.DataFrame(data, columns=colunas)

    # --- AQUI ESTÁ O QUE VOCÊ PEDIU: Criando a coluna Moeda ---
    # Usamos .replace para tirar o 'USDT' e ficar só 'BTC', 'ETH', etc.
    df_temp['Moeda'] = par.replace('USDT', '')

    # Adicionamos este DataFrame na nossa lista de espera
    lista_frames.append(df_temp)
    
    # Boa prática: esperar 1 segundo entre as chamadas para não ser banido pela Binance
    time.sleep(1)

# --- 3. CONSOLIDAÇÃO ---

# Juntamos todos os DataFrames da lista em um só (um embaixo do outro)
df_total = pd.concat(lista_frames, ignore_index=True)

# Agora aplicamos as conversões que você já conhece no blocão todo
cols_to_numeric = ["Open Price", "High Price", "Low Price", "Close Price", "Volume"]
for col in cols_to_numeric:
    df_total[col] = pd.to_numeric(df_total[col])

df_total['Date'] = pd.to_datetime(df_total['Open Time'], unit='ms')

# Selecionamos as colunas finais, incluindo a nova coluna 'Moeda'
df_final = df_total[['Date', 'Moeda', 'Open Price', 'High Price', 'Low Price', 'Close Price', 'Volume', 'Number of Trades']]

# --- 4. CARGA NO AZURE ---
print(f"Salvando {len(df_final)} linhas no Azure SQL...")

try:
    # 'replace' para manter o banco sempre atualizado com os últimos 1000 dias de cada moeda
    df_final.to_sql('tb_binance_historico', con=engine, if_exists='replace', index=False)
    print("✅ Sucesso! Todas as moedas foram carregadas.")
except Exception as e:
    print(f"❌ Erro: {e}")