import pandas as pd
import requests
from sqlalchemy import create_engine
import urllib
import time 
import os

# --- 1. CONFIGURAÇÃO (Mantém igual) ---
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
    f'Encrypt=yes;' # Adicione isto explicitamente
    f'TrustServerCertificate=yes;'
    f'Connection Timeout=30;' # Aumentamos o tempo de espera
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# --- 2. EXTRAÇÃO MULTI-MOEDAS ---
moedas = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'ADAUSDT']
url = "https://api1.binance.com/api/v3/klines"
lista_frames = []

print(f"--- Iniciando Extração Binance (Data: {pd.Timestamp.now()}) ---")

for par in moedas:
    params_api = {
        'symbol': par,
        'interval': '1d',
        'limit': '1000'
    }

    try:
        response = requests.get(url, params=params_api)
        data = response.json()
        
        # LOG DE SEGURANÇA: Verifica se a API não retornou erro
        if isinstance(data, dict) and 'code' in data:
            print(f"⚠️ Alerta API na moeda {par}: {data}")
            continue

        print(f"Moeda: {par} | Linhas recebidas da API: {len(data)}")

        colunas = [
            "Open Time", "Open Price", "High Price", "Low Price", "Close Price",
            "Volume", "Close Time", "Quote Asset Volume", "Number of Trades",
            "Taker Buy Base", "Taker Buy Quote", "Ignore"
        ]
        
        # Criando o DataFrame temporário
        df_temp = pd.DataFrame(data, columns=colunas)
        df_temp['Moeda'] = par.replace('USDT', '')
        
        # Adicionando à lista se não estiver vazio
        if not df_temp.empty:
            lista_frames.append(df_temp)
            print(f"   -> {par} adicionado à lista de espera. Total na lista agora: {len(lista_frames)}")
    
    except Exception as e:
        print(f"❌ Erro ao processar {par}: {e}")
    
    time.sleep(1)

# --- 3. CONSOLIDAÇÃO (Atenção aqui!) ---
print(f"--- Consolidando {len(lista_frames)} moedas ---")

if len(lista_frames) > 0:
    df_total = pd.concat(lista_frames, ignore_index=True)
    print(f"Total de linhas no df_total: {len(df_total)}")
    
    # Transformações
    cols_to_numeric = ["Open Price", "High Price", "Low Price", "Close Price", "Volume"]
    for col in cols_to_numeric:
        df_total[col] = pd.to_numeric(df_total[col])

    df_total['Date'] = pd.to_datetime(df_total['Open Time'], unit='ms')
    
    df_final = df_total[['Date', 'Moeda', 'Open Price', 'High Price', 'Low Price', 'Close Price', 'Volume', 'Number of Trades']]
else:
    df_final = pd.DataFrame() # Cria um DF vazio se a lista estiver vazia

# --- 4. CARGA COM TRAVA ---
print(f"Tentando salvar {len(df_final)} linhas totais no Azure...")

if df_final.empty:
    raise ValueError(f"ERRO CRÍTICO: O DataFrame final está vazio. Lista_frames tinha {len(lista_frames)} itens.")

try:
    df_final.to_sql('tb_binance_historico', con=engine, if_exists='replace', index=False)
    print("✅ SUCESSO ABSOLUTO! Dados carregados no Azure.")
except Exception as e:
    print(f"❌ Erro fatal no SQL: {e}")
    raise e