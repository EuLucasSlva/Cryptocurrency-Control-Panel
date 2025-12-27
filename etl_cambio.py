import pandas as pd
import requests
from sqlalchemy import create_engine
import urllib
import time
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

url ="https://economia.awesomeapi.com.br/last/USD-BRL"

response = requests.get(url)
data = response.json()

# A API retorna os dados dentro da chave 'USDBRL'
dados_dolar = data['USDBRL']

# Criando o DataFrame a partir do dicionário (usamos [dados_dolar] para virar uma linha)
df = pd.DataFrame([dados_dolar])

cols_to_numeric = ["high", "low", "bid", "ask", "varBid", "pctChange"]
for col in cols_to_numeric:
    df[col] = pd.to_numeric(df[col])

print(df.head())

try:
    df.to_sql('tb_cotacao_usdt', con=engine, if_exists = 'replace', index=False)
    print("✅ Sucesso! a cotação foi carregada.")
except Exception as e:
   print(f"❌ Erro: {e}")






