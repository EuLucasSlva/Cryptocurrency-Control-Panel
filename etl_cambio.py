import pandas as pd
import requests
from sqlalchemy import create_engine
import urllib
import os
import time
import time
from sqlalchemy import text

#CONFIGURAÇÃO E VALIDAÇÃO DE AMBIENTE
SERVER = os.getenv('DB_SERVER')
DATABASE = os.getenv('DB_NAME')
USERNAME = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASS')

print(f"--- Iniciando Conexão ---")
print(f"Servidor: {SERVER}")
print(f"Banco: {DATABASE}")
print(f"Usuário: {USERNAME}")

if not all([SERVER, DATABASE, USERNAME, PASSWORD]):
    raise ValueError("ERRO: Uma ou mais variáveis de ambiente (Secrets) não foram encontradas no GitHub!")

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

def buscar_dolar():
    try:
        url = "https://economia.awesomeapi.com.br/last/USD-BRL"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return {"bid": data['USDBRL']['bid'], "source": "AwesomeAPI"}
    except Exception as e:
        print(f"AwesomeAPI falhou: {e}. Tentando reserva...")

    try:
        url_alt = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json"
        response = requests.get(url_alt)
        data = response.json()
        return {"bid": data['usd']['brl'], "source": "FallbackAPI"}
    except Exception as e:
        raise Exception(f"Todas as APIs de câmbio falharam: {e}")

resultado = buscar_dolar()
print(f"Dados obtidos via {resultado['source']} | Valor: {resultado['bid']}")

df = pd.DataFrame([{
    "moeda": "USD",
    "bid": float(resultado['bid']),
    "data_consulta": pd.Timestamp.now()
}])

max_retries = 3
wait_seconds = 45

for i in range(1, max_retries + 1):
    try:
        print(f"Tentativa {i}: Conectando ao Azure...")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        df.to_sql('tb_cotacao_usdt', con=engine, if_exists='replace', index=False)
        print("Banco acordado e dados carregados!")
        break 
    except Exception as e:
        if i < max_retries:
            print(f"Banco dormindo. Aguardando {wait_seconds}s para a próxima tentativa...")
            time.sleep(wait_seconds)
        else:
            print("Erro definitivo após retentativas.")
            raise e