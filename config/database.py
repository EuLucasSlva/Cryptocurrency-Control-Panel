import os
import urllib.parse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import time


class DatabaseConfig:
    
    def __init__(self):
        self.server = os.getenv('DB_SERVER')
        self.database = os.getenv('DB_NAME')
        self.username = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASS')
        
        self._validate_credentials()
        
    def _validate_credentials(self):
        if not all([self.server, self.database, self.username, self.password]):
            raise ValueError("Missing database credentials in environment variables")
    
    def get_engine(self) -> Engine:
        params = urllib.parse.quote_plus(
            f'DRIVER={{ODBC Driver 18 for SQL Server}};'
            f'SERVER={self.server};'
            f'DATABASE={self.database};'
            f'UID={self.username};'
            f'PWD={self.password};'
            f'Encrypt=yes;'
            f'TrustServerCertificate=yes;'
            f'Connection Timeout=180;'
        )
        return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    
    def test_connection(self, max_retries: int = 3, wait_seconds: int = 45) -> bool:
        engine = self.get_engine()
        
        for attempt in range(1, max_retries + 1):
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return True
            except Exception as e:
                if attempt < max_retries:
                    time.sleep(wait_seconds)
                else:
                    raise e
        return False
