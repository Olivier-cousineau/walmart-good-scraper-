"""Entrypoint GitHub Actions pour lancer le scraper Walmart Canada.

Ce script lit la configuration depuis les variables d'environnement (ou le
fichier `.env` généré dans le workflow) afin de piloter le scraper avec des
valeurs sûres par défaut. Il permet notamment de régler le mode headless, le
nombre de magasins par province et la clé API 2Captcha.
"""

from __future__ import annotations

import logging
import os
from typing import Dict, List

from walmart_canada_scraper import WalmartCanadaScraper

logger = logging.getLogger(__name__)


def _as_bool(value: str | None, default: bool) -> bool:
    """Convertir une chaîne en booléen avec fallback."""
    if value is None or value == "":
        return default
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def _as_int(value: str | None, default: int, name: str) -> int:
    """Convertir une chaîne en int, avec journalisation en cas d'échec."""
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Valeur '%s' invalide pour %s, utilisation de %s", value, name, default)
        return default


def load_configuration() -> Dict[str, object]:
    """Charger la configuration depuis les variables d'environnement."""
    proxies = [proxy.strip() for proxy in os.environ.get("PROXIES", "").split(",") if proxy.strip()]
    config: Dict[str, object] = {
        "captcha_api_key": os.environ.get("CAPTCHA_API_KEY"),
        "proxies": proxies,
        "stores_per_province": _as_int(os.environ.get("STORES_PER_PROVINCE"), 2, "STORES_PER_PROVINCE"),
        "output_file": os.environ.get("OUTPUT_FILE", "walmart_canada_results.csv"),
        "headless": _as_bool(os.environ.get("HEADLESS_MODE"), True),
        "max_retries": _as_int(os.environ.get("MAX_RETRIES"), 3, "MAX_RETRIES"),
    }
    return config


def run_scraper(config: Dict[str, object]) -> None:
    """Exécuter le scraper avec la configuration fournie."""
    proxy_list: List[str] = config["proxies"]  # type: ignore[assignment]
    scraper = WalmartCanadaScraper(
        proxy_list=proxy_list,
        captcha_api_key=config["captcha_api_key"],  # type: ignore[arg-type]
        headless=config["headless"],  # type: ignore[arg-type]
        max_retries=config["max_retries"],  # type: ignore[arg-type]
    )

    logger.info(
        "Démarrage du scraping - output=%s, stores/province=%s, headless=%s",
        config["output_file"],
        config["stores_per_province"],
        config["headless"],
    )

    scraper.scrape_all_stores(
        output_file=config["output_file"],  # type: ignore[arg-type]
        stores_per_province=config["stores_per_province"],  # type: ignore[arg-type]
    )


if __name__ == "__main__":
    configuration = load_configuration()
    run_scraper(configuration)
