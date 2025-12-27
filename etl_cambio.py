import pandas as pd
import requests
from sqlalchemy import create_engine
import urllib
import os
import time

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
    f'TrustServerCertificate=yes;'
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# --- 2. EXTRAÇÃO COM FALLBACK (A SOLUÇÃO) ---

def buscar_dolar():
    # Tentativa 1: AwesomeAPI (Sua favorita)
    try:
        url = "https://economia.awesomeapi.com.br/last/USD-BRL"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return {"bid": data['USDBRL']['bid'], "source": "AwesomeAPI"}
    except:
        print("AwesomeAPI falhou, tentando reserva...")

    # Tentativa 2: Fallback (API Reserva do GitHub - Mais estável para Actions)
    try:
        url_alt = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json"
        response = requests.get(url_alt)
        data = response.json()
        return {"bid": data['usd']['brl'], "source": "FallbackAPI"}
    except Exception as e:
        raise Exception(f"Todas as APIs falharam: {e}")

# Executa a busca inteligente
resultado = buscar_dolar()
print(f"Dados obtidos via {resultado['source']}")

# Criando o DataFrame com o valor padronizado
df = pd.DataFrame([{
    "moeda": "USD",
    "bid": float(resultado['bid']),
    "data_consulta": pd.Timestamp.now()
}])

# --- 3. CARGA ---
try:
    df.to_sql('tb_cotacao_usdt', con=engine, if_exists='replace', index=False)
    print(f"✅ Sucesso! Valor de R$ {resultado['bid']} carregado.")
except Exception as e:
    print(f"❌ Erro no SQL: {e}")