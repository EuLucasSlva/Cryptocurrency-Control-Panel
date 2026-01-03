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

acoes_br = [
    "VALE3.SA",  # Vale
    "PETR4.SA",  # Petrobras PN
    "ITUB4.SA",  # Itaú Unibanco
    "PETR3.SA",  # Petrobras ON
    "BBAS3.SA",  # Banco do Brasil
    "B3SA3.SA",  # B3
    "ELET3.SA",  # Eletrobras
    "ABEV3.SA",  # Ambev
    "RENT3.SA",  # Localiza
    "WEGE3.SA",  # Weg
    "ITSA4.SA",  # Itaúsa
    "BPAC11.SA", # BTG Pactual
    "SUZB3.SA",  # Suzano
    "HAPV3.SA",  # Hapvida
    "RDOR3.SA",  # Rede D'Or
    "^BVSP"      # O próprio índice IBOVESPA
]

print("--- Iniciando Download da B3 ---")


dados = yf.download(acoes_br, period="2y")["Close"]


df_final = dados.reset_index().melt(
    id_vars=['Date'], 
    var_name='Ticker', 
    value_name='Close'
)

#Limpeza e padronização
df_final['Ticker'] = df_final['Ticker'].str.replace('.SA','',regex=False)
df_final['Close'] = df_final['close'].round(2)
df_final.dropna(inplace=True)

print(f"Transformação concluída. Amostra:\n{df_final.head()}")
print(f"Total de linhas para carga: {len(df_final)}")

#Carregamento no Azure
try:
    print("Enviando para o Azure SQL...")
    df_final.to_sql('tb_acoes_br_historico', con=engine, if_exists='replace', index=False)
    print("✅ SUCESSO! Dados da B3 carregados.")
except Exception as e:
    print(f"❌ Erro fatal no SQL: {e}")
    raise e