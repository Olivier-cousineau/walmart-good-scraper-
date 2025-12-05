import asyncio
import json
import os
import random
from typing import Any, Dict, Optional

import httpx
from loguru import logger as log
from parsel import Selector
try:
    from scrapfly import ScrapeConfig, ScrapflyClient
except ImportError:
    ScrapflyClient = None
    ScrapeConfig = None

SCRAPFLY_KEY = os.getenv("SCRAPFLY_KEY")
SCRAPFLY_CLIENT = (
    ScrapflyClient(key=SCRAPFLY_KEY) if SCRAPFLY_KEY and ScrapflyClient is not None else None
)

MOBILE_UAS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; SM-G996B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
]

BLOCK_MARKERS = [
    "Robot or human",
    "captcha",
    "blocked",
    "/blocked?",
    "Request blocked",
]


async def fetch_html(
    url: str,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 3,
) -> str | None:
    """
    Fetch HTML from a Walmart URL with retries.

    - Si SCRAPFLY_KEY est défini -> utilise ScrapFly (bypass anti-bot).
    - Sinon -> fallback sur httpx + user-agent mobile + petits délais.
    """
    for attempt in range(1, max_attempts + 1):
        ua = random.choice(MOBILE_UAS)
        log.debug(f"Fetching {url} (attempt {attempt}) with UA: {ua}")

        try:
            # --- Mode ScrapFly (prioritaire si clé dispo) ---
            if SCRAPFLY_CLIENT:
                loop = asyncio.get_running_loop()

                def _scrapfly_call():
                    return SCRAPFLY_CLIENT.scrape(
                        ScrapeConfig(
                            url=url,
                            asp=True,      # anti-scraping bypass
                            country="US",  # proxy US
                            render_js=False,
                            headers={"user-agent": ua},
                        )
                    )

                result = await loop.run_in_executor(None, _scrapfly_call)
                status = result.response_status
                text = result.content

            # --- Mode httpx (fallback sans ScrapFly) ---
            else:
                if client is None:
                    raise RuntimeError(
                        "httpx.AsyncClient is required when SCRAPFLY_KEY is not set"
                    )

                resp = await client.get(
                    url,
                    headers={"user-agent": ua},
                    timeout=30,
                )
                status = resp.status_code
                text = resp.text

            # --- Analyse du blocage ---
            if status in (403, 429, 456, 503) or any(m.lower() in text.lower() for m in BLOCK_MARKERS):
                log.warning(
                    f"Blocked or invalid response for {url} on attempt {attempt}. "
                    f"status={status}, snippet={text[:200]!r}"
                )
                await asyncio.sleep(random.uniform(1.5, 4.5))
                continue

            if "__NEXT_DATA__" not in text:
                log.warning(
                    f"No __NEXT_DATA__ in response from {url} on attempt {attempt}. "
                    f"status={status}, snippet={text[:200]!r}"
                )
                await asyncio.sleep(random.uniform(1.5, 4.5))
                continue

            # ✅ Réponse valide
            return text

        except Exception as e:
            log.error(f"Error fetching {url} on attempt {attempt}: {e}")
            await asyncio.sleep(random.uniform(1.5, 4.5))

    log.error(f"Failed to fetch valid content from {url} after {max_attempts} attempts")
    return None


def extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    selector = Selector(html)
    raw_json = selector.css("script#__NEXT_DATA__::text").get()
    if not raw_json:
        return None
    try:
        return json.loads(raw_json)
    except json.JSONDecodeError:
        log.error("Failed to decode __NEXT_DATA__ JSON")
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
