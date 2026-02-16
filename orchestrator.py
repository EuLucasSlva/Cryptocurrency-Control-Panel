import sys
from typing import List, Type
from etl import (
    BaseETL,
    CurrencyETL,
    NewsETL,
    BrazilianStocksETL,
    NasdaqStocksETL,
    CryptoETL
)
from utils.logger import ETLLogger


class ETLOrchestrator:
    
    def __init__(self):
        self.logger = ETLLogger("Orchestrator")
        self.etl_pipeline: List[Type[BaseETL]] = [
            CurrencyETL,
            NewsETL,
            BrazilianStocksETL,
            NasdaqStocksETL,
            CryptoETL
        ]
        self.results = {}
        
    def run_all(self):
        self.logger.info("Starting ETL orchestration")
        total = len(self.etl_pipeline)
        
        for idx, etl_class in enumerate(self.etl_pipeline, 1):
            etl_name = etl_class.__name__
            self.logger.info(f"[{idx}/{total}] Running {etl_name}")
            
            try:
                etl = etl_class()
                etl.execute()
                self.results[etl_name] = "SUCCESS"
                
            except Exception as e:
                self.logger.error(f"{etl_name} failed: {str(e)}")
                self.results[etl_name] = f"FAILED: {str(e)}"
        
        self._print_summary()
        
    def run_specific(self, etl_names: List[str]):
        self.logger.info(f"Running specific ETLs: {etl_names}")
        
        etl_map = {etl.__name__: etl for etl in self.etl_pipeline}
        
        for name in etl_names:
            if name not in etl_map:
                self.logger.warning(f"ETL '{name}' not found, skipping")
                continue
                
            try:
                etl = etl_map[name]()
                etl.execute()
                self.results[name] = "SUCCESS"
                
            except Exception as e:
                self.logger.error(f"{name} failed: {str(e)}")
                self.results[name] = f"FAILED: {str(e)}"
        
        self._print_summary()
    
    def _print_summary(self):
        self.logger.info("=" * 60)
        self.logger.info("ETL EXECUTION SUMMARY")
        self.logger.info("=" * 60)
        
        success_count = sum(1 for status in self.results.values() if status == "SUCCESS")
        total_count = len(self.results)
        
        for etl_name, status in self.results.items():
            self.logger.info(f"{etl_name}: {status}")
        
        self.logger.info("=" * 60)
        self.logger.info(f"Total: {success_count}/{total_count} successful")
        self.logger.info("=" * 60)


def main():
    orchestrator = ETLOrchestrator()
    
    if len(sys.argv) > 1:
        etl_names = sys.argv[1:]
        orchestrator.run_specific(etl_names)
    else:
        orchestrator.run_all()


if __name__ == "__main__":
    main()
