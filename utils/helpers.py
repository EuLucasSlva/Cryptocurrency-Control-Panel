import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, Any
from config.settings import RETRY_CONFIG


def create_robust_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=RETRY_CONFIG['max_retries'],
        backoff_factor=RETRY_CONFIG['backoff_factor'],
        status_forcelist=RETRY_CONFIG['status_forcelist'],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def normalize_ticker(ticker: str, remove_suffix: str = None) -> str:
    if remove_suffix:
        return ticker.replace(remove_suffix, '')
    return ticker


def safe_api_call(session: requests.Session, url: str, params: Dict[str, Any] = None, 
                  headers: Dict[str, str] = None, timeout: int = None) -> Dict[str, Any]:
    timeout = timeout or RETRY_CONFIG['timeout']
    headers = headers or {'User-Agent': 'Mozilla/5.0'}
    
    response = session.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()
