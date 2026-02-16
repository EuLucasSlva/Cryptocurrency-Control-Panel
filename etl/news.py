import pandas as pd
import feedparser
from datetime import datetime
from .base_etl import BaseETL
from config.settings import NEWS_SEARCH_TERMS, API_CONFIGS, DATABASE_TABLES


class NewsETL(BaseETL):
    
    def __init__(self):
        super().__init__("NewsETL")
        self.news_list = []
        
    def extract(self):
        self.logger.info("Fetching news from Google News RSS")
        
        for item in NEWS_SEARCH_TERMS:
            self._fetch_news_for_asset(item)
    
    def _fetch_news_for_asset(self, item: dict):
        try:
            self.logger.info(f"Fetching news for {item['ticker']}")
            search_term = item['query'].replace(' ', '%20')
            rss_url = f"{API_CONFIGS['google_news_base_url']}?q={search_term}&hl=pt-BR&gl=BR&ceid=BR:pt-419"
            
            feed = feedparser.parse(rss_url)
            
            for entry in feed.entries[:6]:
                self.news_list.append(self._parse_news_entry(entry, item))
                
            self.logger.info(f"Fetched {min(6, len(feed.entries))} news items for {item['ticker']}")
            
        except Exception as e:
            self.logger.error(f"Failed to fetch news for {item['ticker']}: {e}")
    
    def _parse_news_entry(self, entry, item: dict) -> dict:
        try:
            dt_pub = datetime(*entry.published_parsed[:6])
        except:
            dt_pub = datetime.now()
        
        return {
            'Data': dt_pub,
            'Ativo': item['ticker'],
            'Tipo': item['tipo'],
            'Titulo': entry.title,
            'Fonte': entry.source.title if 'source' in entry else 'Google News',
            'Link': entry.link,
            'UUID': entry.id if 'id' in entry else None
        }
    
    def transform(self):
        self.logger.info("Transforming news data")
        self.df = pd.DataFrame(self.news_list)
        
        if not self.df.empty:
            self.df = self.df.sort_values(by='Data', ascending=False)
            
            yesterday = datetime.now() - pd.Timedelta(days=1)
            self.df = self.df[self.df['Data'] > yesterday]
            
            self.logger.info(f"Filtered to {len(self.df)} recent news items")
        
    def load(self):
        if self.df.empty:
            self.logger.warning("No recent news to save")
            return
            
        self.save_to_database(DATABASE_TABLES['news'], if_exists='append')


if __name__ == "__main__":
    etl = NewsETL()
    etl.execute()
