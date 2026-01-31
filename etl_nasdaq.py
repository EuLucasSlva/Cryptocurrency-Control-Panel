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

acoes_usa = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", 
    "AVGO", "COST", "NFLX", "AMD", "PEP", "ADBE", "CSCO", "TMUS", "^NDX"
]

print(f"--- Iniciando Download da NASDAQ ({len(acoes_usa)} ativos) ---")

dados = yf.download(acoes_usa, period="2y")[["Open", "High", "Low", "Close"]]

df_stack = dados.stack(level=1).reset_index()
df_stack.columns = ['Date', 'Ticker', 'Close', 'High', 'Low', 'Open']

cols_numeric = ['Open', 'High', 'Low', 'Close']
df_stack[cols_numeric] = df_stack[cols_numeric].round(2)
df_stack.dropna(subset=['Close'], inplace=True)
df_stack['Mercado'] = 'NASDAQ'

print(f"Transformação concluída. Amostra:\n{df_stack.head()}")

try:
    print("Enviando para o Azure SQL...")
    df_stack.to_sql('tb_acoes_nasdaq_historico', con=engine, if_exists='replace', index=False)
    print("✅ SUCESSO! Dados da NASDAQ (OHLC) carregados.")
except Exception as e:
    print(f"❌ Erro fatal no SQL: {e}")
    raise e