import pandas as pd
import feedparser
from sqlalchemy import create_engine
import urllib
import os
from datetime import datetime
import time

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

ativos_busca = [
    {'ticker': 'BTC-USD', 'tipo': 'CRIPTO', 'query': 'Bitcoin BTC preço mercado'},
    {'ticker': '^IXIC',   'tipo': 'INDICE', 'query': 'Nasdaq bolsa valores'},
    {'ticker': '^BVSP',   'tipo': 'INDICE', 'query': 'Ibovespa ações brasil'}
]

def buscar_noticias_google():
    lista_noticias = []
    print("--- Iniciando Coleta de Notícias (Google News RSS) ---")
    
    for item in ativos_busca:
        try:
            termo = item['query'].replace(' ', '%20')
            rss_url = f"https://news.google.com/rss/search?q={termo}&hl=pt-BR&gl=BR&ceid=BR:pt-419"
            
            print(f"Buscando notícias para: {item['ticker']}...")
            feed = feedparser.parse(rss_url)
    
            for entry in feed.entries[:6]:
                try:
                    dt_pub = datetime(*entry.published_parsed[:15])
                except:
                    dt_pub = datetime.now()

                lista_noticias.append({
                    'Data': dt_pub,
                    'Ativo': item['ticker'],  
                    'Tipo': item['tipo'],    
                    'Titulo': entry.title,
                    'Fonte': entry.source.title if 'source' in entry else 'Google News',
                    'Link': entry.link,
                    'UUID': entry.id if 'id' in entry else None 
                })
        except Exception as e:
            print(f"Erro ao buscar {item['ticker']}: {e}")
            
    return pd.DataFrame(lista_noticias)

df_news = buscar_noticias_google()

if not df_news.empty:
    print(f"\nColetadas {len(df_news)} notícias no total.")

    df_news = df_news.sort_values(by='Data', ascending=False)

    ontem = datetime.now() - pd.Timedelta(days=1)
    df_news_recentes = df_news[df_news['Data'] > ontem]
    
    if not df_news_recentes.empty:
        print(f"\n--- PREVIEW DE SALVAMENTO ({len(df_news_recentes)} itens) ---")
        print(df_news_recentes[['Data', 'Ativo', 'Titulo']].head())
        
        print(f"\nSalvando {len(df_news_recentes)} notícias recentes no SQL Server...")
        
        try:
            df_news_recentes.to_sql('tb_noticias_mercado', con=engine, if_exists='append', index=False)
            print("SUCESSO: Notícias salvas no banco!")
            
        except Exception as e:
            print(f"ERRO AO SALVAR NO BANCO: {e}")
    else:
        print("Nenhuma notícia nova (nas últimas 24h) para salvar.")
else:
    print("Nenhuma notícia encontrada.")