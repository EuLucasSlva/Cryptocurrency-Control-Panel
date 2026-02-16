# Market Data Pipeline

Automated ETL pipeline for collecting financial market data including cryptocurrencies, stocks, exchange rates, and market news.

## Architecture

```
market_data_pipeline/
├── config/              # Configuration and database connection
├── etl/                 # ETL modules (Extract, Transform, Load)
├── utils/               # Utilities (logging, helpers)
├── logs/                # Application logs
├── orchestrator.py      # Main execution controller
└── requirements.txt     # Python dependencies
```

## Features

- **Cryptocurrency Data**: Top 15 cryptos with market cap and volume from CoinGecko/Binance
- **Brazilian Stocks**: Ibovespa and main B3 stocks (2-year historical data)
- **NASDAQ Stocks**: Top tech stocks and NASDAQ index (2-year historical data)
- **Exchange Rates**: USD/BRL daily rates
- **Market News**: Latest news from Google News RSS feed

## Data Sources

- CoinGecko API (primary for crypto)
- Binance Vision API (fallback for crypto)
- Yahoo Finance API (stocks)
- AwesomeAPI (currency rates)
- Google News RSS (market news)

## Setup

### Local Development

1. Clone the repository
```bash
git clone <your-repo-url>
cd market_data_pipeline
```

2. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Configure environment variables
```bash
cp .env.example .env
# Edit .env with your database credentials
```

### GitHub Actions Setup

Configure the following secrets in your repository:
- `DB_SERVER`: Azure SQL Server address
- `DB_NAME`: Database name
- `DB_USER`: Database username
- `DB_PASS`: Database password

## Usage

### Run all ETLs
```bash
python orchestrator.py
```

### Run specific ETLs
```bash
python orchestrator.py CurrencyETL NewsETL
```

### Run individual ETL
```bash
python -m etl.currency
python -m etl.crypto
python -m etl.brazilian_stocks
python -m etl.nasdaq_stocks
python -m etl.news
```

## Database Schema

### tb_binance_historico
- Date, Moeda, Open Price, High Price, Low Price, Close Price, Volume, Number of Trades, Market_Cap

### tb_acoes_br_historico
- Date, Ticker, Open, High, Low, Close, Volume

### tb_acoes_nasdaq_historico
- Date, Ticker, Open, High, Low, Close, Volume, Mercado

### tb_cotacao_usdt
- moeda, bid, data_consulta

### tb_noticias_mercado
- Data, Ativo, Tipo, Titulo, Fonte, Link, UUID

## Logging

Logs are stored in the `logs/` directory with format:
- Console output: Real-time execution status
- File logs: Daily rotating logs per ETL module

## Error Handling

- Automatic retry logic for API failures
- Fallback APIs for critical data sources
- Graceful degradation (continues on partial failures)
- Comprehensive error logging

## Scheduled Execution

GitHub Actions runs the pipeline:
- **Schedule**: Every Monday at 9:00 AM UTC
- **Manual**: Via workflow_dispatch trigger

## Design Principles

### 1. Separation of Concerns
- Configuration separated from business logic
- Each ETL is independent and self-contained
- Utilities are reusable across modules

### 2. DRY (Don't Repeat Yourself)
- Base ETL class eliminates code duplication
- Centralized database connection management
- Shared logging and error handling

### 3. Single Responsibility
- Each ETL handles one data source
- Clear Extract-Transform-Load separation
- Orchestrator only manages execution flow

### 4. Open/Closed Principle
- Easy to add new ETL modules without modifying existing code
- Extensible through inheritance (BaseETL)

### 5. Dependency Injection
- Database configuration injected via environment variables
- No hardcoded credentials

## Contributing

1. Create new ETL modules by inheriting from `BaseETL`
2. Add configuration to `config/settings.py`
3. Register in orchestrator
4. Add tests for new functionality

## License

MIT License
