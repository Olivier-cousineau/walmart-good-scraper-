import argparse
import asyncio
import csv
import json
from pathlib import Path
from typing import Any, Dict, List

from loguru import logger

from .walmart_product import WalmartProductScraper
from .walmart_search import WalmartSearchScraper
from .utils import ensure_product_url

OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def save_json(data: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    logger.info(f"Saved JSON: {path}")


def save_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "id",
        "name",
        "price",
        "rating",
        "reviews_count",
        "availability",
        "image",
        "product_url",
    ]
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Saved CSV: {path}")


def build_csv_rows(search_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in search_results:
        rows.append(
            {
                "id": item.get("id"),
                "name": item.get("name"),
                "price": item.get("price"),
                "rating": item.get("rating"),
                "reviews_count": item.get("reviews"),
                "availability": item.get("availability"),
                "image": item.get("image"),
                "product_url": ensure_product_url(item.get("url", "")),
            }
        )
    return rows


async def run_scraper(query: str, pages: int, concurrency: int) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    search_scraper = WalmartSearchScraper()
    product_scraper = WalmartProductScraper(concurrency=concurrency)

    try:
        search_results = await search_scraper.scrape_search(query, pages)
        save_json(search_results, OUTPUT_DIR / "walmart_search.json")

        product_urls = [result.get("url", "") for result in search_results]
        product_details = await product_scraper.scrape_products(product_urls)
        save_json(product_details, OUTPUT_DIR / "walmart_products.json")

        csv_rows = build_csv_rows(search_results)
        save_csv(csv_rows, OUTPUT_DIR / "walmart_products.csv")

    finally:
        await asyncio.gather(search_scraper.close(), product_scraper.close())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Asynchronous Walmart.com scraper")
    parser.add_argument("--query", required=True, help="Search query to scrape")
    parser.add_argument("--pages", type=int, default=1, help="Number of search pages to scrape (max 25)")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrent product detail requests")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger.add(lambda msg: print(msg, end=""), level="INFO")
    asyncio.run(run_scraper(args.query, args.pages, args.concurrency))


if __name__ == "__main__":
    main()
