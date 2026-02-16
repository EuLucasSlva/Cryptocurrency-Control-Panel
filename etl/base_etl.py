from abc import ABC, abstractmethod
import pandas as pd
from sqlalchemy.engine import Engine
from config.database import DatabaseConfig
from utils.logger import ETLLogger


class BaseETL(ABC):
    
    def __init__(self, name: str):
        self.name = name
        self.logger = ETLLogger(name)
        self.db_config = DatabaseConfig()
        self.engine: Engine = None
        self.df: pd.DataFrame = None
        
    def execute(self):
        try:
            self.logger.info(f"Starting {self.name} ETL process")
            
            self._setup()
            self.extract()
            self.transform()
            self.load()
            
            self.logger.success(f"{self.name} ETL completed successfully")
            
        except Exception as e:
            self.logger.error(f"{self.name} ETL failed: {str(e)}", exc_info=True)
            raise
        finally:
            self._cleanup()
    
    def _setup(self):
        self.logger.info("Setting up database connection")
        self.engine = self.db_config.get_engine()
        
    def _cleanup(self):
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database connection closed")
    
    @abstractmethod
    def extract(self):
        pass
    
    @abstractmethod
    def transform(self):
        pass
    
    @abstractmethod
    def load(self):
        pass
    
    def save_to_database(self, table_name: str, if_exists: str = 'replace'):
        if self.df is None or self.df.empty:
            self.logger.warning("No data to save")
            return
            
        self.logger.info(f"Saving {len(self.df)} rows to {table_name}")
        self.df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
        self.logger.success(f"Data saved to {table_name}")
