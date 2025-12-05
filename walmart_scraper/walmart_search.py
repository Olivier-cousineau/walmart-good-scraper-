import asyncio
import urllib.parse
from typing import Any, Dict, List, Optional

import httpx
from loguru import logger

from .utils import ensure_product_url, extract_next_data, fetch_html, normalize_price, safe_get


class WalmartSearchScraper:
    def __init__(self, client: Optional[httpx.AsyncClient] = None) -> None:
        self._client = client or httpx.AsyncClient(follow_redirects=True)

    async def close(self) -> None:
        await self._client.aclose()

    async def fetch_search_page(self, query: str, page: int) -> List[Dict[str, Any]]:
        encoded_query = urllib.parse.quote_plus(query)
        url = f"https://www.walmart.com/search?q={encoded_query}&page={page}"
        html = await fetch_html(url, self._client)
        if not html:
            logger.error(f"No HTML retrieved for search page {page}")
            return []

        next_data = extract_next_data(html)
        if not next_data:
            logger.error(f"Unable to parse __NEXT_DATA__ for search page {page}")
            return []

        items = safe_get(
            next_data,
            "props",
            "pageProps",
            "initialData",
            "searchResult",
            "itemStacks",
        )
        if not isinstance(items, list) or not items:
            logger.warning(f"No itemStacks found for page {page}")
            return []

        first_stack = items[0]
        raw_items = first_stack.get("items", []) if isinstance(first_stack, dict) else []
        parsed_items = [self._parse_item(item) for item in raw_items]
        return [item for item in parsed_items if item]

    def _parse_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None

        price = normalize_price(item.get("priceInfo") or item.get("price"))
        rating = safe_get(item, "averageRating") or safe_get(item, "rating")
        reviews = safe_get(item, "numberOfReviews") or safe_get(item, "reviews")
        availability = safe_get(item, "availabilityStatus") or safe_get(item, "availability")

        url = ensure_product_url(item.get("canonicalUrl") or item.get("productPageUrl", ""))

        return {
            "id": item.get("usItemId") or item.get("id"),
            "name": item.get("title") or item.get("name"),
            "price": price,
            "rating": rating,
            "reviews": reviews,
            "availability": availability,
            "image": safe_get(item, "imageInfo", "thumbnailUrl") or item.get("image"),
            "url": url,
        }

    async def scrape_search(self, query: str, pages: int = 1, max_pages: int = 25) -> List[Dict[str, Any]]:
        total_pages = min(pages, max_pages)
        results: List[Dict[str, Any]] = []

        for page in range(1, total_pages + 1):
            logger.info(f"Scraping search page {page}/{total_pages} for query '{query}'")
            page_items = await self.fetch_search_page(query, page)
            if not page_items:
                logger.info(f"No items found on page {page}, stopping pagination")
                break
            results.extend(page_items)
            await asyncio.sleep(0.5)

        logger.info(f"Collected {len(results)} search results")
        return results
