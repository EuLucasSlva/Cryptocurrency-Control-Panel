import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
import urllib
import os

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
    f'Connection Timeout=180;'
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

acoes_br = [
    "VALE3.SA", "PETR4.SA", "ITUB4.SA", "BBDC4.SA", "BBAS3.SA", 
    "B3SA3.SA", "ELET3.SA", "ABEV3.SA", "RENT3.SA", "WEGE3.SA", 
    "ITSA4.SA", "BPAC11.SA", "SUZB3.SA", "HAPV3.SA", "RDOR3.SA", "^BVSP"
]

print("--- Iniciando Download da B3 (OHLC) ---")

dados = yf.download(acoes_br, period="2y")[["Open", "High", "Low", "Close"]]

df_stack = dados.stack(level=1).reset_index()
df_stack.columns = ['Date', 'Ticker', 'Close', 'High', 'Low', 'Open']

df_stack['Ticker'] = df_stack['Ticker'].str.replace('.SA', '', regex=False)
cols_numeric = ['Open', 'High', 'Low', 'Close']
df_stack[cols_numeric] = df_stack[cols_numeric].round(2)
df_stack.dropna(subset=['Close'], inplace=True)

print(f"Transformação concluída. Amostra:\n{df_stack.head()}")

try:
    print("Enviando para o Azure SQL...")
    df_stack.to_sql('tb_acoes_br_historico', con=engine, if_exists='replace', index=False)
    print("✅ SUCESSO! Dados da B3 (OHLC) carregados.")
except Exception as e:
    print(f"❌ Erro fatal no SQL: {e}")
    raise e