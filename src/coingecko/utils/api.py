import time
import logging
from datetime import datetime

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://api.coingecko.com/api/v3"
RATE_LIMIT_SLEEP = 2  # seconds between coin requests


class CoinGeckoClient:
    def __init__(self, retries: int = 5, backoff: int = 10):
        self.retries = retries
        self.backoff = backoff
        self.session = requests.Session()

    def _get(self, url: str, params: dict) -> dict:
        for attempt in range(self.retries):
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = self.backoff * (attempt + 1)
                log.warning(f"Rate limit hit. Waiting {wait}s (attempt {attempt + 1}/{self.retries})")
                time.sleep(wait)
            else:
                resp.raise_for_status()
        raise RuntimeError(f"Failed after {self.retries} attempts: GET {url}")

    def get_top_coins(self, n: int = 50) -> list[dict]:
        return self._get(f"{BASE_URL}/coins/markets", {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": n,
            "page": 1,
            "sparkline": False,
        })

    def get_coin_history(self, coin_id: str, dt_from: int, dt_to: int) -> list[dict]:
        data = self._get(f"{BASE_URL}/coins/{coin_id}/market_chart/range", {
            "vs_currency": "usd",
            "from": dt_from,
            "to": dt_to,
        })

        prices = data.get("prices", [])
        market_caps = data.get("market_caps", [])
        volumes = data.get("total_volumes", [])

        records = []
        for i in range(len(prices)):
            ts_ms = prices[i][0]
            records.append({
                "timestamp": datetime.fromtimestamp(ts_ms / 1000),
                "current_price": prices[i][1],
                "market_cap": market_caps[i][1] if i < len(market_caps) else None,
                "total_volume": volumes[i][1] if i < len(volumes) else None,
            })

        time.sleep(RATE_LIMIT_SLEEP)
        return records
