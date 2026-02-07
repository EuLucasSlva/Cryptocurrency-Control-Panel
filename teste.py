import feedparser
import pandas as pd
from datetime import datetime
import time

# --- CONFIGURAÇÃO DOS ATIVOS ---
# Desta vez, usamos termos de busca para o Google News, que é mais assertivo
ativos_busca = [
    {'nome': 'Bitcoin',          'query': 'Bitcoin BTC preço mercado'},
    {'nome': 'Nasdaq',           'query': 'Nasdaq bolsa valores'},
    {'nome': 'Ibovespa',         'query': 'Ibovespa bolsa brasil ações'}
]

def buscar_google_news():
    lista_noticias = []
    print("\n🌍 Conectando ao Google News...")

    for item in ativos_busca:
        # Codifica o termo de busca para URL (ex: Bitcoin%20BTC...)
        termo = item['query'].replace(' ', '%20')
        
        # URL do RSS do Google News (Brasil - pt-BR)
        rss_url = f"https://news.google.com/rss/search?q={termo}&hl=pt-BR&gl=BR&ceid=BR:pt-419"
        
        try:
            feed = feedparser.parse(rss_url)
            print(f"-> Buscando: {item['nome']} (Encontradas: {len(feed.entries)})")

            # Pega as 5 notícias mais recentes de cada ativo
            for entry in feed.entries[:2]:
                # Tenta converter a data de publicação
                try:
                    dt_pub = datetime(*entry.published_parsed[:6])
                except:
                    dt_pub = datetime.now()

                lista_noticias.append({
                    'Data': dt_pub,
                    'Ativo': item['nome'],
                    'Titulo': entry.title,
                    'Fonte': entry.source.title if 'source' in entry else 'Google News',
                    'Link': entry.link
                })
        except Exception as e:
            print(f"❌ Erro ao buscar {item['nome']}: {e}")

    return pd.DataFrame(lista_noticias)

# --- EXECUÇÃO E VISUALIZAÇÃO ---
df = buscar_google_news()

if not df.empty:
    # Ordena por data
    df = df.sort_values(by='Data', ascending=False)

    print("\n" + "="*100)
    print(f"📢 FEED DE MERCADO - GOOGLE NEWS ({len(df)} notícias)")
    print("="*100)

    for _, row in df.iterrows():
        data_fmt = row['Data'].strftime('%d/%m %H:%M')
        print(f"⏰ {data_fmt} | 🏷️  {row['Ativo']}")
        print(f"📰 {row['Titulo']}")
        print(f"🔗 {row['Link']}")
        print("-" * 100)
else:
    print("\nNenhuma notícia encontrada (Verifique sua conexão com a internet).")