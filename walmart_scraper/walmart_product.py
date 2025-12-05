import asyncio
from typing import Any, Dict, List, Optional

import httpx
from loguru import logger

from .utils import extract_next_data, fetch_html, safe_get


class WalmartProductScraper:
    def __init__(self, client: Optional[httpx.AsyncClient] = None, concurrency: int = 10) -> None:
        self._client = client or httpx.AsyncClient(follow_redirects=True)
        self._semaphore = asyncio.Semaphore(concurrency)

    async def close(self) -> None:
        await self._client.aclose()

    async def fetch_product(self, url: str) -> Optional[Dict[str, Any]]:
        async with self._semaphore:
            html = await fetch_html(url, self._client)
            if not html:
                logger.error(f"No HTML retrieved for product {url}")
                return None

            next_data = extract_next_data(html)
            if not next_data:
                logger.error(f"Unable to parse __NEXT_DATA__ for product {url}")
                return None

            product_data = safe_get(next_data, "props", "pageProps", "initialData", "data", "product")
            reviews_data = safe_get(next_data, "props", "pageProps", "initialData", "data", "reviews")
            if not isinstance(product_data, dict):
                logger.warning(f"No product data found for {url}")
                return None

            product_data["reviews"] = reviews_data
            return product_data

    async def scrape_products(self, urls: List[str]) -> List[Dict[str, Any]]:
        tasks = [self.fetch_product(url) for url in urls]
        results = await asyncio.gather(*tasks)
        filtered = [result for result in results if result]
        logger.info(f"Fetched {len(filtered)} product detail pages")
        return filtered
