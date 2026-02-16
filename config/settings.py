CRYPTO_ASSETS = [
    {'binance': 'BTCUSDT', 'coingecko': 'bitcoin'},
    {'binance': 'ETHUSDT', 'coingecko': 'ethereum'},
    {'binance': 'BNBUSDT', 'coingecko': 'binancecoin'},
    {'binance': 'SOLUSDT', 'coingecko': 'solana'},
    {'binance': 'XRPUSDT', 'coingecko': 'ripple'},
    {'binance': 'DOGEUSDT', 'coingecko': 'dogecoin'},
    {'binance': 'ADAUSDT', 'coingecko': 'cardano'},
    {'binance': 'TRXUSDT', 'coingecko': 'tron'},
    {'binance': 'AVAXUSDT', 'coingecko': 'avalanche-2'},
    {'binance': 'SHIBUSDT', 'coingecko': 'shiba-inu'},
    {'binance': 'DOTUSDT', 'coingecko': 'polkadot'},
    {'binance': 'LINKUSDT', 'coingecko': 'chainlink'},
    {'binance': 'LTCUSDT', 'coingecko': 'litecoin'},
    {'binance': 'NEARUSDT', 'coingecko': 'near'},
    {'binance': 'BTCUSDT', 'coingecko': 'tether'}
]

BRAZILIAN_STOCKS = [
    "VALE3.SA", "PETR4.SA", "ITUB4.SA", "BBDC4.SA", "BBAS3.SA",
    "B3SA3.SA", "ELET3.SA", "ABEV3.SA", "RENT3.SA", "WEGE3.SA",
    "ITSA4.SA", "BPAC11.SA", "SUZB3.SA", "HAPV3.SA", "RDOR3.SA", "^BVSP"
]

NASDAQ_STOCKS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
    "AVGO", "COST", "NFLX", "AMD", "PEP", "ADBE", "CSCO", "TMUS", "^NDX"
]

NEWS_SEARCH_TERMS = [
    {'ticker': 'BTC-USD', 'tipo': 'CRIPTO', 'query': 'Bitcoin BTC preço mercado'},
    {'ticker': '^IXIC', 'tipo': 'INDICE', 'query': 'Nasdaq bolsa valores'},
    {'ticker': '^BVSP', 'tipo': 'INDICE', 'query': 'Ibovespa ações brasil'}
]

API_CONFIGS = {
    'coingecko_base_url': 'https://api.coingecko.com/api/v3',
    'binance_base_url': 'https://data-api.binance.vision/api/v3',
    'awesomeapi_url': 'https://economia.awesomeapi.com.br/last/USD-BRL',
    'fallback_currency_url': 'https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json',
    'google_news_base_url': 'https://news.google.com/rss/search'
}

RETRY_CONFIG = {
    'max_retries': 5,
    'backoff_factor': 2,
    'status_forcelist': [429, 500, 502, 503, 504],
    'timeout': 10,
    'wait_between_requests': 12
}

DATABASE_TABLES = {
    'crypto': 'tb_binance_historico',
    'brazilian_stocks': 'tb_acoes_br_historico',
    'nasdaq_stocks': 'tb_acoes_nasdaq_historico',
    'currency': 'tb_cotacao_usdt',
    'news': 'tb_noticias_mercado'
}
