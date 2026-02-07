import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
import urllib
import os
import datetime

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

ativos_noticias = [
    {'ticker': 'BTC-USD', 'tipo': 'CRIPTO', 'nome': 'Bitcoin'},
    {'ticker': 'ETH-USD', 'tipo': 'CRIPTO', 'nome': 'Ethereum'},
    {'ticker': 'BRL=X',   'tipo': 'FIAT',   'nome': 'Dolar vs Real'}, 
    {'ticker': '^IXIC',   'tipo': 'INDICE', 'nome': 'Nasdaq Composite'},
    {'ticker': '^BVSP',   'tipo': 'INDICE', 'nome': 'Ibovespa'}
]

def buscar_noticias_yahoo():
    lista_noticias = []
    
    print("--- Iniciando Coleta de Notícias ---")
    
    for item in ativos_noticias:
        try:
            print(f"Buscando notícias para: {item['nome']} ({item['ticker']})...")
            ticker_obj = yf.Ticker(item['ticker'])
            news = ticker_obj.news
            
            for n in news:
                pub_date = pd.to_datetime(n.get('providerPublishTime'), unit='s')
                
                lista_noticias.append({
                    'Data': pub_date,
                    'Ativo': item['ticker'],
                    'Tipo': item['tipo'],
                    'Titulo': n.get('title'),
                    'Fonte': n.get('publisher'),
                    'Link': n.get('link'),
                    'UUID': n.get('uuid') 
                })
        except Exception as e:
            print(f"Erro ao buscar {item['ticker']}: {e}")
            
    return pd.DataFrame(lista_noticias)

df_news = buscar_noticias_yahoo()

if not df_news.empty:
    print(f"\nColetadas {len(df_news)} notícias.")

    df_news = df_news.sort_values(by='Data', ascending=False)

    ontem = pd.Timestamp.now() - pd.Timedelta(days=1)
    df_news_recentes = df_news[df_news['Data'] > ontem]
    
    print(f"Salvando {len(df_news_recentes)} notícias recentes no SQL...")
    
    try:
        df_news_recentes.to_sql('tb_noticias_mercado', con=engine, if_exists='append', index=False)
        print("Notícias salvas com sucesso!")
        
    except Exception as e:
        print(f"Erro ao salvar no banco: {e}")
else:
    print("Nenhuma notícia encontrada.")