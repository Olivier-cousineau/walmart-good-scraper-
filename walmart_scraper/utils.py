import asyncio
import json
import random
from typing import Any, Dict, List, Optional

import httpx
from loguru import logger
from parsel import Selector

MOBILE_USER_AGENTS: List[str] = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Mobile/15E148 Safari/604.1",
]

BLOCK_STRINGS = ["Robot or human", "blocked", "blocked your request"]


def build_headers(user_agent: str) -> Dict[str, str]:
    return {
        "User-Agent": user_agent,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
    }


def has_next_data(html: str) -> bool:
    return "__NEXT_DATA__" in html


def extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    selector = Selector(html)
    raw_json = selector.css("script#__NEXT_DATA__::text").get()
    if not raw_json:
        return None
    try:
        return json.loads(raw_json)
    except json.JSONDecodeError:
        logger.error("Failed to decode __NEXT_DATA__ JSON")
        return None


def is_blocked(response: httpx.Response, html: str) -> bool:
    if response.status_code == 456:
        return True
    lower_html = html.lower()
    for pattern in BLOCK_STRINGS:
        if pattern.lower() in lower_html:
            return True
    return not has_next_data(html)


async def fetch_html(
    client: httpx.AsyncClient,
    url: str,
    max_retries: int = 3,
    min_delay: float = 1.5,
    max_delay: float = 4.5,
) -> Optional[str]:
    for attempt in range(1, max_retries + 1):
        user_agent = random.choice(MOBILE_USER_AGENTS)
        headers = build_headers(user_agent)
        logger.debug(f"Fetching {url} (attempt {attempt}) with UA: {user_agent}")
        try:
            response = await client.get(url, headers=headers, timeout=30)
            html = response.text
        except httpx.HTTPError as exc:
            logger.warning(f"HTTP error fetching {url}: {exc}")
            html = ""

        if html and not is_blocked(response if 'response' in locals() else httpx.Response(0), html):
            return html

        logger.warning(
            f"Blocked or invalid response for {url} on attempt {attempt}. Retrying after delay."
        )
        await asyncio.sleep(random.uniform(min_delay, max_delay))

    logger.error(f"Failed to fetch valid content from {url} after {max_retries} attempts")
    return None


def ensure_product_url(url: str) -> str:
    if url.startswith("http"):
        return url
    return f"https://www.walmart.com{url}"


def normalize_price(price_info: Any) -> Optional[float]:
    if not price_info:
        return None
    if isinstance(price_info, (int, float)):
        return float(price_info)
    if isinstance(price_info, dict):
        for key in ["price", "minPrice", "maxPrice", "priceDisplay"]:
            value = price_info.get(key)
            if isinstance(value, (int, float)):
                return float(value)
        current = price_info.get("currentPrice") or price_info.get("current")
        if isinstance(current, dict):
            amount = current.get("price") or current.get("amount")
            if isinstance(amount, (int, float)):
                return float(amount)
    return None


def safe_get(data: Dict[str, Any], *keys: str) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current
