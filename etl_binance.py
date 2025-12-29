import pandas as pd
import requests
from sqlalchemy import create_engine
import urllib
import os

# --- 1. CONFIGURAÇÃO DO BANCO (Com melhorias de Timeout) ---
SERVER = os.getenv('DB_SERVER')
DATABASE = os.getenv('DB_NAME')
USERNAME = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASS')

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

# --- 2. EXTRAÇÃO VIA COINCAP (Cloud Friendly) ---

# IDs das moedas na CoinCap
moedas_ids = ['bitcoin', 'ethereum', 'solana', 'cardano']
lista_frames = []

print(f"--- Iniciando Extração CoinCap (Resiliente a bloqueios) ---")

for crypto in moedas_ids:
    url = f"https://api.coincap.io/v2/assets/{crypto}/history?interval=d1"
    
    try:
        response = requests.get(url)
        data = response.json()
        
        if 'data' in data:
            df_temp = pd.DataFrame(data['data'])
            # Padronizando colunas para manter compatibilidade com seu BI
            df_temp['Moeda'] = crypto.upper()
            df_temp['Date'] = pd.to_datetime(df_temp['time'], unit='ms')
            df_temp['Close Price'] = pd.to_numeric(df_temp['priceUsd'])
            
            # Criamos colunas vazias apenas para não quebrar seu Power BI atual
            df_temp['Open Price'] = df_temp['Close Price']
            df_temp['High Price'] = df_temp['Close Price']
            df_temp['Low Price'] = df_temp['Close Price']
            df_temp['Volume'] = 0
            df_temp['Number of Trades'] = 0

            lista_frames.append(df_temp)
            print(f"✅ {crypto.capitalize()} extraído com sucesso.")
        else:
            print(f"⚠️ Falha ao obter dados de {crypto}: {data}")
            
    except Exception as e:
        print(f"❌ Erro na extração de {crypto}: {e}")

# --- 3. CONSOLIDAÇÃO ---
if lista_frames:
    df_final = pd.concat(lista_frames, ignore_index=True)
    df_final = df_final[['Date', 'Moeda', 'Open Price', 'High Price', 'Low Price', 'Close Price', 'Volume', 'Number of Trades']]
else:
    df_final = pd.DataFrame()

# --- 4. CARGA NO AZURE ---
print(f"Tentando salvar {len(df_final)} linhas no Azure...")

if df_final.empty:
    raise ValueError("ERRO CRÍTICO: Nenhuma moeda foi extraída. Verifique a conexão com a API.")

try:
    df_final.to_sql('tb_binance_historico', con=engine, if_exists='replace', index=False)
    print("✅ SUCESSO! Dados carregados via CoinCap.")
except Exception as e:
    print(f"❌ Erro fatal no SQL: {e}")
    raise e