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
    f'Connection Timeout=60;'
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

acoes_usa = [
    "AAPL",  # Apple
    "MSFT",  # Microsoft
    "NVDA",  # NVIDIA
    "GOOGL", # Alphabet (Google)
    "AMZN",  # Amazon
    "META",  # Meta (Facebook)
    "TSLA",  # Tesla
    "AVGO",  # Broadcom
    "COST",  # Costco
    "NFLX",  # Netflix
    "AMD",   # AMD
    "PEP",   # PepsiCo
    "ADBE",  # Adobe
    "CSCO",  # Cisco
    "TMUS",  # T-Mobile
    "^NDX"   # Índice NASDAQ-100 
]

print(f"--- Iniciando Download da NASDAQ ({len(acoes_usa)} ativos) ---")


dados = yf.download(acoes_usa, period="2y")["Close"]

df_final = dados.reset_index().melt(
    id_vars=['Date'], 
    var_name='Ticker', 
    value_name='Close'
)


df_final['Close'] = df_final['Close'].round(2)
df_final.dropna(inplace=True)

df_final['Mercado'] = 'NASDAQ'

print(f"Transformação concluída. Amostra:\n{df_final.head()}")


try:
    print("Enviando para o Azure SQL...")
    df_final.to_sql('tb_acoes_nasdaq_historico', con=engine, if_exists='replace', index=False)
    print("✅ SUCESSO! Dados da NASDAQ carregados.")
except Exception as e:
    print(f"❌ Erro fatal no SQL: {e}")
    raise e