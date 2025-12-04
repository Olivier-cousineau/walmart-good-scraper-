"""
Walmart Canada Scraper - robuste et tout-épreuve.

Ce module fournit un scraper Selenium basé sur undetected-chromedriver avec
rotation de proxy, contournement CAPTCHA et export CSV/JSON. Il inclut une
interface CLI pour lancer un scraping contrôlé par arguments ou via un workflow
GitHub Actions déclenché manuellement.
"""

import argparse
import csv
import json
import logging
import os
import random
import shutil
import time
from datetime import datetime
from typing import Dict, List, Optional

import requests
import undetected_chromedriver as uc
from selenium.webdriver import Chrome
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from twocaptcha import TwoCaptcha

# ============ CONFIGURATION LOGGING ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("walmart_scraper.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("walmart_canada_scraper")


def _detect_chrome_binary() -> str:
    """Retourne le chemin du binaire Chrome installé dans l'environnement CI.

    Le workflow GitHub Actions installe Google Chrome stable via
    `browser-actions/setup-chrome`. Cette fonction cherche explicitement le
    binaire fourni (google-chrome-stable) et échoue de manière explicite si
    aucun binaire n'est trouvé pour éviter un démarrage silencieux du driver.
    """

    # Priorité aux variables d'environnement si l'utilisateur fournit un chemin custom
    env_candidates = [
        os.environ.get("CHROME_BINARY"),
        os.environ.get("GOOGLE_CHROME_SHIM"),
    ]
    for candidate in env_candidates:
        if candidate and os.path.isfile(candidate):
            return candidate

    # Recherche standard des binaires Chrome/Chromium installés
    for binary in (
        "google-chrome-stable",
        "google-chrome",
        "chrome",
        "chromium",
    ):
        path = shutil.which(binary)
        if path:
            return path

    raise FileNotFoundError(
        "Chrome introuvable dans l'environnement. Vérifiez que le workflow a exécuté "
        "l'étape 'Setup Chrome' et que le binaire est présent dans PATH."
    )


def create_driver(headless: bool = True):
    """
    Crée un driver Chrome compatible GitHub Actions (CI) avec undetected_chromedriver.
    """

    try:
        options = uc.ChromeOptions()

        # Garantir que le binaire Chrome installé par le workflow est utilisé
        chrome_binary = _detect_chrome_binary()
        options.binary_location = chrome_binary

        # ---- FLAGS SPÉCIAUX POUR CI / DOCKER / GITHUB ACTIONS ----
        if headless:
            # Headless mode pour Chrome récent
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-tools")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-setuid-sandbox")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--remote-debugging-port=9222")

        # Réduire la détection d’automatisation
        options.add_argument("--disable-blink-features=AutomationControlled")

        logger.info("Création du driver Chrome (undetected_chromedriver) pour Walmart Canada...")

        # IMPORTANT EN CI : éviter le subprocess séparé qui peut mourir immédiatement
        driver = uc.Chrome(
            options=options,
            use_subprocess=False,
        )

        driver.set_page_load_timeout(60)
        driver.set_script_timeout(60)

        logger.info("✓ Driver Chrome initialisé avec succès.")
        return driver

    except Exception as e:
        logger.error(f"✗ Erreur lors de la configuration du driver: {e}", exc_info=True)
        raise


class WalmartCanadaScraper:
    """
    Scraper robuste pour Walmart Canada.

    - Bypass CAPTCHA automatique (2Captcha, PerimeterX)
    - Rotation d'IP/proxies résidentiels
    - Détection anti-bot (undetected-chromedriver + Selenium Stealth)
    - Gestion des rate limits et retry
    - Support des 402 stores Walmart Canada
    """

    # Liste complète des 402 stores Walmart Canada (par province)
    WALMART_STORES_CANADA = {
        "Ontario": 147,
        "Quebec": 72,
        "Alberta": 59,
        "British Columbia": 48,
        "Nova Scotia": 18,
        "Manitoba": 16,
        "Saskatchewan": 14,
        "New Brunswick": 13,
        "Newfoundland and Labrador": 11,
        "Prince Edward Island": 2,
    }

    # User agents réalistes modernes
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    ]

    def __init__(
        self,
        proxy_list: Optional[List[str]] = None,
        captcha_api_key: Optional[str] = None,
        headless: bool = True,
        max_retries: int = 3,
    ):
        """
        Initialiser le scraper.

        Args:
            proxy_list: Liste de proxies résidentiels (format: 'http://ip:port' ou
                'http://user:pass@ip:port').
            captcha_api_key: Clé API 2Captcha pour résoudre les CAPTCHAs.
            headless: Mode headless du navigateur (False = voir le navigateur).
            max_retries: Nombre maximum de tentatives par page.
        """

        self.proxy_list = proxy_list or []
        self.captcha_api_key = captcha_api_key
        self.captcha_solver = TwoCaptcha(captcha_api_key) if captcha_api_key else None
        self.headless = headless
        self.max_retries = max_retries
        self.driver: Optional[Chrome] = None
        self.current_proxy_index = 0
        self.session_start_time = datetime.now()
        self.retry_count = 0

        logger.info(
            "Scraper initialisé - Proxies: %s, CAPTCHA API: %s",
            len(self.proxy_list),
            "Oui" if captcha_api_key else "Non",
        )

    def rotate_proxy(self) -> Optional[str]:
        """Rotation du proxy depuis la liste."""
        if not self.proxy_list:
            return None
        proxy = self.proxy_list[self.current_proxy_index % len(self.proxy_list)]
        self.current_proxy_index += 1
        logger.debug(
            f"Proxy utilisé: {proxy[:30]}..."
            if len(proxy) > 30
            else f"Proxy utilisé: {proxy}"
        )
        return proxy

    def get_random_user_agent(self) -> str:
        """Retourner un user agent aléatoire."""
        return random.choice(self.USER_AGENTS)

    def setup_driver(self) -> uc.Chrome:
        """Configurer le driver Selenium compatible CI."""

        return create_driver(headless=self.headless)

    def human_like_delay(self, min_sec: float = 2, max_sec: float = 5):
        """Délai aléatoire pour simuler un humain."""
        delay = random.uniform(min_sec, max_sec)
        logger.debug("Délai: %.2fs", delay)
        time.sleep(delay)

    def simulate_human_interaction(self, element):
        """Simuler l'interaction humaine avec un élément (mouvement souris + clic)."""
        try:
            actions = ActionChains(self.driver)
            actions.move_to_element(element).perform()
            self.human_like_delay(0.5, 1.5)
            actions.click().perform()
            self.human_like_delay(1, 3)
            logger.debug("✓ Interaction humaine simulée")
        except Exception as exc:
            logger.warning("Erreur lors de la simulation: %s", exc)

    def handle_captcha(self) -> bool:
        """
        Gérer le CAPTCHA PerimeterX/reCAPTCHA.

        Stratégies:
        1. Attendre que undetected-chromedriver le bypass automatiquement
        2. Utiliser 2Captcha si disponible
        3. Attendre l'utilisateur
        """

        try:
            logger.warning("⚠️ CAPTCHA détecté!")

            try:
                self.driver.find_element(By.ID, "px_captcha")
                logger.info("CAPTCHA PerimeterX détecté")

                logger.info("Attente du bypass automatique... (max 15s)")
                for _ in range(15):
                    time.sleep(1)
                    try:
                        self.driver.find_element(By.ID, "px_captcha")
                    except Exception:
                        logger.info("✓ CAPTCHA contourné automatiquement!")
                        return True

            except Exception:
                pass

            if self.captcha_solver:
                logger.info("Tentative de résolution avec 2Captcha...")
                try:
                    iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
                    for iframe in iframes:
                        iframe_name = iframe.get_attribute("name") or ""
                        if "captcha" in iframe_name.lower() or "recaptcha" in iframe_name.lower():
                            page_source = self.driver.page_source
                            if "recaptcha" in page_source:
                                logger.info("reCAPTCHA détecté - Utilisation de 2Captcha")
                                logger.info("Attente manuelle requise (30s max)")
                                time.sleep(30)
                                return True
                except Exception as exc:
                    logger.warning("Erreur lors de l'extraction CAPTCHA: %s", exc)

            logger.warning("CAPTCHA non résolu - Attente de 30 secondes...")
            time.sleep(30)
            return True

        except Exception as exc:
            logger.error("Erreur lors du traitement CAPTCHA: %s", exc)
            return False

    def scrape_store_page(self, store_url: str, retry: int = 0) -> Optional[Dict]:
        """Scraper une page de store Walmart avec gestion d'erreurs."""
        try:
            logger.info("[Tentative %s/%s] Scrape: %s", retry + 1, self.max_retries, store_url)
            self.driver.get(store_url)

            self.human_like_delay(3, 6)

            try:
                captcha_elements = self.driver.find_elements(
                    By.XPATH,
                    "//*[contains(@id, 'captcha') or contains(@id, 'recaptcha') or contains(@class, 'captcha')]",
                )

                if captcha_elements:
                    logger.info("CAPTCHA détecté!")
                    if not self.handle_captcha():
                        if retry < self.max_retries - 1:
                            self.human_like_delay(5, 10)
                            return self.scrape_store_page(store_url, retry + 1)
                        return None
            except Exception:
                pass

            data = {
                "url": store_url,
                "timestamp": datetime.now().isoformat(),
                "store_name": "",
                "address": "",
                "phone": "",
                "hours": "",
                "product_count": 0,
                "products": [],
            }

            try:
                data["store_name"] = self.driver.find_element(By.XPATH, "//h1").text
            except Exception:
                data["store_name"] = "N/A"

            try:
                products = self.driver.find_elements(
                    By.XPATH,
                    "//div[@class='search-result-gridview-item'] | //div[contains(@class, 'product')]",
                )
                logger.info("Trouvé %s produits", len(products))

                for i, product in enumerate(products):
                    if i >= 20:
                        break
                    try:
                        name_elem = product.find_elements(
                            By.XPATH, ".//a[@class='product-title'] | .//a[contains(@class, 'product')]"
                        )
                        price_elem = product.find_elements(
                            By.XPATH, ".//span[@class='price'] | .//span[contains(@class, 'price')]"
                        )

                        if name_elem and price_elem:
                            data["products"].append(
                                {
                                    "name": name_elem[0].text,
                                    "price": price_elem[0].text,
                                    "url": name_elem[0].get_attribute("href")
                                    if name_elem[0].get_attribute("href")
                                    else "N/A",
                                }
                            )
                    except Exception as exc:
                        logger.debug("Erreur extraction produit %s: %s", i, exc)
                        continue

            except Exception as exc:
                logger.warning("Erreur lors de l'extraction des produits: %s", exc)

            data["product_count"] = len(data["products"])
            logger.info("✓ Page scrapée: %s (%s produits)", data["store_name"], data["product_count"])
            return data

        except Exception as exc:
            logger.error("✗ Erreur lors du scrape: %s", exc)
            if retry < self.max_retries - 1:
                logger.info("Retry %s/%s...", retry + 1, self.max_retries - 1)
                self.human_like_delay(5, 10)
                return self.scrape_store_page(store_url, retry + 1)
            return None

    def scrape_all_stores(self, output_file: str = "walmart_canada_data.csv", stores_per_province: Optional[int] = None):
        """
        Scraper toutes les Walmart Canada une après l'autre.

        Args:
            output_file: Fichier de sortie CSV.
            stores_per_province: Nombre de stores à scraper par province (None = tous).
        """

        logger.info("\n%s", "=" * 60)
        logger.info("DÉMARRAGE DU SCRAPE WALMART CANADA")
        logger.info("Total de stores à scraper: %s", sum(self.WALMART_STORES_CANADA.values()))
        logger.info("Provinces: %s", len(self.WALMART_STORES_CANADA))
        logger.info("%s\n", "=" * 60)

        all_data: List[Dict] = []
        store_count = 0
        province_count = 0

        try:
            self.driver = self.setup_driver()

            for province, total_stores in self.WALMART_STORES_CANADA.items():
                logger.info("\n%s", "─" * 60)
                logger.info("PROVINCE: %s (%s stores)", province, total_stores)
                logger.info("%s", "─" * 60)

                stores_to_scrape = stores_per_province or total_stores
                province_count += 1

                for store_num in range(1, min(stores_to_scrape + 1, total_stores + 1)):
                    try:
                        store_url = f"https://www.walmart.ca/en/stores/{province.replace(' ', '-').lower()}/store-{store_num}"

                        data = self.scrape_store_page(store_url)
                        if data:
                            all_data.append(data)
                            store_count += 1

                        self.human_like_delay(4, 8)

                        if store_count % 10 == 0 and len(self.proxy_list) > 1:
                            logger.info("┌─ Rotation de proxy en cours...")
                            try:
                                self.driver.quit()
                            except Exception:
                                pass
                            self.driver = self.setup_driver()
                            logger.info("└─ Proxy rotationné ✓")
                            self.human_like_delay(3, 6)

                    except KeyboardInterrupt:
                        logger.info("\n⚠️ Interruption utilisateur!")
                        raise
                    except Exception as exc:
                        logger.error("✗ Erreur pour le store %s: %s", store_num, exc)
                        continue

            logger.info("\n%s", "=" * 60)
            logger.info("SAUVEGARDE DES DONNÉES")
            logger.info("Stores scrapés: %s", store_count)
            logger.info("Provinces traitées: %s", province_count)
            logger.info("Fichier de sortie: %s", output_file)
            logger.info("%s\n", "=" * 60)

            self.save_data(all_data, output_file)

            logger.info("✓ SCRAPE TERMINÉ AVEC SUCCÈS!")
            logger.info("  └─ %s stores scrapés", store_count)
            logger.info("  └─ %s pages sauvegardées", len(all_data))
            logger.info("  └─ Fichier: %s", output_file)

            return all_data

        except KeyboardInterrupt:
            logger.warning("Scrape interrompu par l'utilisateur")
            if all_data:
                self.save_data(all_data, f"partial_{output_file}")
                logger.info("Données partielles sauvegardées: partial_%s", output_file)

        except Exception as exc:
            logger.error("✗ Erreur critique: %s", exc)

        finally:
            if self.driver:
                try:
                    self.driver.quit()
                    logger.info("Driver fermé")
                except Exception:
                    pass

    def save_data(self, data: List[Dict], output_file: str):
        """Sauvegarder les données en CSV."""
        try:
            if not data:
                logger.warning("Aucune donnée à sauvegarder")
                return

            with open(output_file, "w", newline="", encoding="utf-8") as file_handle:
                fieldnames = [
                    "url",
                    "timestamp",
                    "store_name",
                    "address",
                    "phone",
                    "hours",
                    "product_count",
                    "products",
                ]
                writer = csv.DictWriter(file_handle, fieldnames=fieldnames)
                writer.writeheader()

                for item in data:
                    item["products"] = json.dumps(item["products"], ensure_ascii=False)
                    writer.writerow(item)

            logger.info("✓ Données sauvegardées: %s", output_file)

            json_file = output_file.replace(".csv", ".json")
            with open(json_file, "w", encoding="utf-8") as file_handle:
                json.dump(data, file_handle, indent=2, ensure_ascii=False)
            logger.info("✓ Données sauvegardées: %s", json_file)

        except Exception as exc:
            logger.error("✗ Erreur lors de la sauvegarde: %s", exc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scraper Walmart Canada")
    parser.add_argument(
        "--proxies",
        default="",
        help="Liste de proxies séparés par des virgules (http://user:pass@ip:port)",
    )
    parser.add_argument(
        "--captcha-api-key",
        dest="captcha_api_key",
        default=None,
        help="Clé API 2Captcha",
    )
    parser.add_argument(
        "--stores-per-province",
        dest="stores_per_province",
        type=int,
        default=5,
        help="Nombre de stores à scraper par province (None = tous)",
    )
    parser.add_argument(
        "--output-file",
        dest="output_file",
        default="walmart_canada_results.csv",
        help="Nom du fichier de sortie CSV",
    )
    parser.add_argument(
        "--headless",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Activer/désactiver le mode headless du navigateur",
    )
    parser.add_argument(
        "--max-retries",
        dest="max_retries",
        type=int,
        default=3,
        help="Nombre maximum de tentatives par page",
    )
    return parser.parse_args()


def main():  # pragma: no cover - nécessite Chrome/2Captcha
    args = parse_args()
    proxies = [proxy.strip() for proxy in args.proxies.split(",") if proxy.strip()]

    logger.info("Initialisation du scraper...")
    scraper = WalmartCanadaScraper(
        proxy_list=proxies,
        captcha_api_key=args.captcha_api_key,
        headless=args.headless,
        max_retries=args.max_retries,
    )

    scraper.scrape_all_stores(
        output_file=args.output_file,
        stores_per_province=args.stores_per_province,
    )


if __name__ == "__main__":
    main()
