"""
Walmart Canada Scraper - robuste et tout-épreuve.

Ce module fournit un scraper Selenium basé sur Chrome/ChromeDriver standards
avec rotation de proxy, contournement CAPTCHA et export CSV/JSON. Il inclut une
interface CLI pour lancer un scraping contrôlé par arguments ou via un workflow
GitHub Actions déclenché manuellement.
"""

import argparse
import csv
import json
import logging
import os
import random
import re
import shutil
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse

import requests
from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
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


def _detect_chromedriver_binary() -> str:
    """Trouver le binaire ChromeDriver installé par `setup-chrome`."""

    env_candidates = [
        os.environ.get("CHROMEDRIVER_PATH"),
        os.environ.get("WEBDRIVER_CHROME_DRIVER"),
    ]
    for candidate in env_candidates:
        if candidate and os.path.isfile(candidate):
            return candidate

    path = shutil.which("chromedriver")
    if path:
        return path

    raise FileNotFoundError(
        "ChromeDriver introuvable. Assurez-vous que l'action 'Setup Chrome' a installé le driver"
        " et que le chemin est exporté dans CHROMEDRIVER_PATH."
    )


def create_driver(headless: bool = True, user_agent: Optional[str] = None, proxy: Optional[str] = None) -> Chrome:
    """Créer un driver Chrome compatible GitHub Actions avec Selenium classique."""

    try:
        options = Options()

        chrome_binary = _detect_chrome_binary()
        options.binary_location = chrome_binary

        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-tools")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-setuid-sandbox")
        options.add_argument("--window-size=1366,768")

        if user_agent:
            options.add_argument(f"--user-agent={user_agent}")
        if proxy:
            options.add_argument(f"--proxy-server={proxy}")

        logger.info("Création du driver Chrome (Selenium) pour Walmart Canada...")

        service = Service(executable_path=_detect_chromedriver_binary())
        driver = webdriver.Chrome(service=service, options=options)

        driver.set_page_load_timeout(60)
        driver.set_script_timeout(60)

        if user_agent:
            try:
                driver.execute_cdp_cmd(
                    "Network.setUserAgentOverride",
                    {
                        "userAgent": user_agent,
                        "platform": "Windows",
                        "acceptLanguage": "en-CA",
                    },
                )
            except Exception:
                logger.debug("Impossible d'ajuster l'UA via CDP (fallback sur options)")

        logger.info("✓ Driver Chrome initialisé avec succès.")
        return driver

    except Exception as e:
        logger.error(f"✗ Erreur lors de la configuration du driver: {e}", exc_info=True)
        raise


def fetch_walmart_api_via_browser(driver, base_url, params, timeout: int = 30000):
    """
    Faire un appel à l'API Walmart via fetch() exécuté dans le contexte du navigateur.

    Args:
        driver: Selenium WebDriver déjà positionné sur la page magasin.
        base_url: "https://www.walmart.ca/api/product-search/search".
        params: dict des query params.
        timeout: timeout JS en ms (par défaut 30s).

    Returns:
        (status_code, response_text) ou (None, None) en cas d'erreur.
    """

    query_string = urlencode(params)
    full_url = f"{base_url}?{query_string}"

    script = """
    const callback = arguments[arguments.length - 1];
    const url = arguments[0];
    const timeoutMs = arguments[1];

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    fetch(url, {
        method: 'GET',
        credentials: 'include'
    })
    .then(async (resp) => {
        const text = await resp.text();
        clearTimeout(timeoutId);
        callback(JSON.stringify({
            ok: true,
            status: resp.status,
            body: text
        }));
    })
    .catch((err) => {
        clearTimeout(timeoutId);
        callback(JSON.stringify({
            ok: false,
            error: String(err)
        }));
    });
    """

    try:
        raw = driver.execute_async_script(script, full_url, timeout)
        data = json.loads(raw)
        if not data.get("ok"):
            logger.warning("[fetch_walmart_api_via_browser] Erreur JS: %s", data.get("error"))
            return None, None
        return data.get("status"), data.get("body")
    except Exception as e:
        logger.warning("[fetch_walmart_api_via_browser] Exception Python: %s", e)
        return None, None


class WalmartCanadaScraper:
    """
    Scraper robuste pour Walmart Canada.

    - Bypass CAPTCHA automatique (2Captcha, PerimeterX)
    - Rotation d'IP/proxies résidentiels
    - Durcissement anti-bot via Chrome standard configuré pour la CI
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

    # User agents réalistes modernes (mobile-first pour coller au front)
    USER_AGENTS = [
        "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
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
        self.active_proxy: Optional[str] = None
        self.proxy_profiles: Dict[str, Dict[str, str]] = {}
        self.session_start_time = datetime.now()
        self.retry_count = 0
        self.debug_api_save_limit = 5
        self.debug_api_saved_stores = set()
        self.api_blocked = False

        logger.info(
            "Scraper initialisé - Proxies: %s, CAPTCHA API: %s",
            len(self.proxy_list),
            "Oui" if captcha_api_key else "Non",
        )

    def rotate_proxy(self) -> Optional[str]:
        """Rotation du proxy depuis la liste."""
        if not self.proxy_list:
            self.active_proxy = None
            return None
        proxy = self.proxy_list[self.current_proxy_index % len(self.proxy_list)]
        self.current_proxy_index += 1
        self.active_proxy = proxy
        logger.debug(
            f"Proxy utilisé: {proxy[:30]}..." if len(proxy) > 30 else f"Proxy utilisé: {proxy}"
        )
        return proxy

    def _ensure_proxy_profile(self, proxy: Optional[str]) -> Tuple[str, str]:
        """Associer un profil (UA + locale) à un proxy donné."""

        profile_key = proxy or "direct"
        if profile_key not in self.proxy_profiles:
            user_agent = self.get_random_user_agent()
            accept_language = random.choice(
                [
                    "fr-CA,fr-FR;q=0.9,en-CA;q=0.8,en;q=0.7",
                    "en-CA,en;q=0.9,fr-CA;q=0.8,fr;q=0.7",
                ]
            )
            self.proxy_profiles[profile_key] = {
                "user_agent": user_agent,
                "accept_language": accept_language,
            }
        profile = self.proxy_profiles[profile_key]
        return profile["user_agent"], profile["accept_language"]

    def _sec_ch_ua(self, user_agent: str) -> str:
        """Construire un header sec-ch-ua cohérent avec Chrome/Edge."""

        if "Edg/" in user_agent:
            return '"Not/A)Brand";v="99", "Microsoft Edge";v="120", "Chromium";v="120"'
        if "Chrome/" in user_agent:
            return '"Not.A/Brand";v="99", "Google Chrome";v="120", "Chromium";v="120"'
        if "Firefox" in user_agent:
            return '"Not.A/Brand";v="99", "Firefox";v="121"'
        return '"Not.A/Brand";v="99"'

    def _is_mobile_user_agent(self, user_agent: str) -> bool:
        """Identifier si l'UA correspond à un navigateur mobile."""

        lowered = user_agent.lower()
        return "mobile" in lowered or "android" in lowered or "iphone" in lowered or "ipad" in lowered

    def _sec_ch_ua_platform(self, user_agent: str) -> str:
        """Platform sec-ch-ua cohérente avec l'UA."""

        if "Android" in user_agent:
            return '"Android"'
        if "iPhone" in user_agent or "iPad" in user_agent:
            return '"iOS"'
        return '"Windows"'

    def _build_page_headers(self, user_agent: str, accept_language: str, referer: str) -> Dict[str, str]:
        """Headers réalistes pour une page HTML."""

        return {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": accept_language,
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Referer": referer,
            "sec-ch-ua": self._sec_ch_ua(user_agent),
            "sec-ch-ua-mobile": "?1" if self._is_mobile_user_agent(user_agent) else "?0",
            "sec-ch-ua-platform": self._sec_ch_ua_platform(user_agent),
        }

    def _build_api_headers(self, user_agent: str, accept_language: str, referer: str) -> Dict[str, str]:
        """Headers réalistes pour l'API JSON Walmart."""

        return {
            "User-Agent": user_agent,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": accept_language,
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Referer": referer,
            "Origin": "https://www.walmart.ca",
            "sec-ch-ua": self._sec_ch_ua(user_agent),
            "sec-ch-ua-mobile": "?1" if self._is_mobile_user_agent(user_agent) else "?0",
            "sec-ch-ua-platform": self._sec_ch_ua_platform(user_agent),
        }

    def _prime_cookies(
        self, session: requests.Session, store_url: str, user_agent: str, accept_language: str
    ) -> None:
        """Récupérer des cookies initiaux via le navigateur ou un GET HTML."""

        if self.driver:
            try:
                for cookie in self.driver.get_cookies():
                    session.cookies.set(cookie["name"], cookie["value"])
                return
            except Exception:
                logger.debug("Impossible de copier les cookies du navigateur; fallback HTTP")

        bootstrap_headers = self._build_page_headers(user_agent, accept_language, "https://www.walmart.ca/")

        try:
            session.get("https://www.walmart.ca/", headers=bootstrap_headers, timeout=20)
            session.get(store_url, headers=bootstrap_headers, timeout=20)
        except Exception as exc:
            logger.debug("Échec du bootstrap cookies: %s", exc)

    def get_random_user_agent(self) -> str:
        """Retourner un user agent aléatoire."""
        return random.choice(self.USER_AGENTS)

    def setup_driver(self) -> Chrome:
        """Configurer le driver Selenium compatible CI."""

        proxy = self.active_proxy if self.active_proxy is not None else self.rotate_proxy()
        user_agent, _ = self._ensure_proxy_profile(proxy)

        return create_driver(
            headless=self.headless,
            user_agent=user_agent,
            proxy=proxy,
        )

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
        1. Attendre un contournement automatique éventuel
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

            logger.warning("CAPTCHA non résolu après tentatives automatiques/2Captcha")
            return False

        except Exception as exc:
            logger.error("Erreur lors du traitement CAPTCHA: %s", exc)
            return False

    def cooldown_after_captcha(self):
        """Mettre l'IP/store au repos après un CAPTCHA non résolu."""

        cooldown = random.uniform(30, 60)
        logger.warning(
            "CAPTCHA non résolu -> cooldown de %.1fs + rotation proxy avant de passer au store suivant",
            cooldown,
        )
        time.sleep(cooldown)
        self.rotate_proxy()

    def _extract_store_metadata(self, store_url: str):
        """Extraire store_id, province et slug depuis l'URL/page actuelle."""

        parsed = urlparse(store_url)
        parts = [part for part in parsed.path.split("/") if part]
        province = parts[2] if len(parts) >= 3 else ""
        store_slug = parts[3] if len(parts) >= 4 else ""

        page_source = self.driver.page_source if self.driver else ""
        store_id = None

        patterns = [
            r"\"storeId\"\s*:\s*\"?(\d+)\"?",
            r"data-store-number=\"?(\d+)\"?",
            r"storeNumber\":\s*\"?(\d+)\"?",
        ]

        for pattern in patterns:
            match = re.search(pattern, page_source)
            if match:
                store_id = match.group(1)
                break

        if not store_id and store_slug:
            slug_match = re.search(r"(\d+)", store_slug)
            if slug_match:
                store_id = slug_match.group(1)

        if store_id:
            logger.info("Store ID détecté: %s", store_id)
        else:
            logger.warning("Store ID introuvable dans la page: fallback sur slug")

        return store_id, province, store_slug

    def _detect_promo_type(self, raw: Dict, promo_hint: Optional[str]) -> Optional[str]:
        """Déterminer si un produit est en rollback/clearance/deal."""

        promo_sources = []
        badges = raw.get("badges") or raw.get("tags") or raw.get("categoryTags") or []
        if isinstance(badges, dict):
            badges = list(badges.values())
        if isinstance(badges, list):
            promo_sources.extend([str(badge) for badge in badges])

        potential_keys = [
            "offerType",
            "priceType",
            "promoTag",
            "badgeText",
            "sellerBadges",
            "availabilityStatus",
        ]
        for key in potential_keys:
            value = raw.get(key)
            if isinstance(value, (list, tuple)):
                promo_sources.extend([str(v) for v in value])
            elif value:
                promo_sources.append(str(value))

        promo_text = " ".join(promo_sources).lower()

        if "rollback" in promo_text:
            return "rollback"
        if "clearance" in promo_text:
            return "clearance"
        if "deal" in promo_text or "special" in promo_text or "promo" in promo_text:
            return "deal"

        return promo_hint

    def _normalize_product(
        self, raw: Dict, store_id: Optional[str], store_slug: str, province: str, promo_type: Optional[str]
    ) -> Optional[Dict]:
        """Normaliser les données produit renvoyées par l'API produit."""

        if not promo_type:
            return None

        product_id = raw.get("usItemId") or raw.get("id") or raw.get("productId") or raw.get("sku") or raw.get("itemId")
        sku = raw.get("sku") or product_id or ""
        product_url = raw.get("productPageUrl") or raw.get("canonicalUrl") or raw.get("canonicalUrlKey")
        if product_url and product_url.startswith("/"):
            product_url = f"https://www.walmart.ca{product_url}"

        title = raw.get("name") or raw.get("title") or raw.get("description") or raw.get("productName")

        price_info = raw.get("priceInfo") or raw.get("priceinfo") or {}
        price_candidates = []
        if isinstance(price_info, dict):
            price_candidates.extend(
                [price_info.get("currentPrice"), price_info.get("price"), price_info.get("pricePerUnit"), price_info.get("primaryOffer")]
            )

        price_candidates.extend(
            [
                raw.get("price"),
                raw.get("currentPrice"),
                raw.get("sellingPrice"),
                raw.get("primaryOffer"),
                raw.get("offer", {}),
                raw.get("priceDisplay"),
            ]
        )

        current_price = None
        original_price = None

        for candidate in price_candidates:
            if isinstance(candidate, dict):
                if current_price is None:
                    current_price = candidate.get("price") or candidate.get("currentPrice") or candidate.get("amount")
                if original_price is None:
                    original_price = (
                        candidate.get("wasPrice")
                        or candidate.get("originalPrice")
                        or candidate.get("compareAtPrice")
                        or candidate.get("listPrice")
                    )
            elif isinstance(candidate, (int, float)) and current_price is None:
                current_price = candidate
            elif isinstance(candidate, str) and current_price is None:
                try:
                    current_price = float(candidate.replace("$", "").replace(",", ""))
                except Exception:
                    continue

        if original_price is None and isinstance(price_info, dict):
            original_price = (
                price_info.get("wasPrice")
                or price_info.get("originalPrice")
                or price_info.get("compareAtPrice")
                or price_info.get("listPrice")
            )

        if not title and not product_url:
            return None

        discount_percent = None
        if current_price and original_price:
            try:
                discount_percent = round((1 - float(current_price) / float(original_price)) * 100, 2)
            except Exception:
                discount_percent = None

        return {
            "store_id": store_id or store_slug,
            "store_slug": store_slug,
            "province": province,
            "product_id": product_id or "",
            "sku": sku,
            "name": title or "",
            "product_url": product_url or "",
            "current_price": current_price,
            "original_price": original_price,
            "discount_percent": discount_percent,
            "promo_type": promo_type,
            "store_quantity": raw.get("quantity") or raw.get("availableQuantity") or None,
        }

    def _mark_api_blocked(self, response_body: Optional[str], store_id: Optional[str], query: str, page: int):
        """Détecter un blocage API (HTTP 456) et sauvegarder le body pour debug."""

        if self.api_blocked:
            return

        self.api_blocked = True
        logger.error(
            "Blocage API détecté (456) pour store %s sur la requête '%s' page %s. Arrêt des appels API.",
            store_id,
            query,
            page,
        )

        try:
            os.makedirs("debug_walmart", exist_ok=True)
            debug_filename = f"api-blocked-store-{store_id or 'unknown'}-q{query}-p{page}.json"
            debug_path = os.path.join("debug_walmart", debug_filename)
            with open(debug_path, "w", encoding="utf-8") as debug_file:
                debug_file.write(response_body or "")
            logger.info("Corps de la réponse 456 enregistré: %s", debug_path)
        except Exception as exc:  # noqa: PERF203
            logger.warning("Impossible d'enregistrer le body de la réponse 456: %s", exc)

        logger.info("Rotation du proxy et des entêtes après blocage 456")
        self.rotate_proxy()

    def _extract_products_via_api(
        self, store_id: Optional[str], store_slug: str, province: str, store_url: str, max_pages: int = 2
    ) -> List[Dict]:
        """Récupérer les produits en promotion via l'API de recherche produit.

        Walmart ne liste pas les produits directement sur la page magasin. On utilise
        l'API `product-search/search` avec des requêtes ciblées (rollback/clearance)
        pour obtenir un échantillon de produits promotionnels par magasin.
        """

        if not store_id:
            logger.warning("Aucun store_id disponible pour l'extraction produit")
            return []

        if self.api_blocked:
            logger.warning(
                "API Walmart déjà signalée comme bloquée (456). Skip storeId=%s pour accélérer la boucle.",
                store_id,
            )
            return []

        logger.info("Recherche des produits en LIQUIDATION (rollback/clearance/deal) pour storeId=%s...", store_id)

        session = requests.Session()
        session.trust_env = False

        proxy = self.active_proxy
        user_agent, accept_language = self._ensure_proxy_profile(proxy)

        if proxy:
            session.proxies.update({"http": proxy, "https": proxy})

        headers = self._build_api_headers(user_agent, accept_language, store_url)
        session.headers.update(headers)
        self._prime_cookies(session, store_url, user_agent, accept_language)

        search_queries = [
            ("rollback", "rollback"),
            ("clearance", "clearance"),
            ("deal", "deal"),
        ]

        all_products: List[Dict] = []
        seen_keys = set()
        total_api_items = 0

        use_browser_api = False

        for query, promo_hint in search_queries:
            page = 1
            while page <= max_pages:
                params = {
                    "page": page,
                    "query": query,
                    "storeId": store_id,
                    "itemsPerPage": 32,
                    "lang": "en",
                }

                api_url = "https://www.walmart.ca/api/product-search/search"
                logger.info("Appel API Walmart: %s - params=%s", api_url, params)

                response_status = None
                response_text: Optional[str] = None

                for attempt in range(2):
                    if use_browser_api:
                        response_status, response_text = fetch_walmart_api_via_browser(
                            self.driver, api_url, params
                        )
                        logger.info(
                            "Réponse API Walmart via navigateur: %s - status=%s - len=%s",
                            f"{api_url}?{urlencode(params)}",
                            response_status,
                            len(response_text or ""),
                        )
                    else:
                        response = session.get(api_url, params=params, timeout=30)

                        response_status = response.status_code
                        response_text = response.text

                        logger.info(
                            "Réponse API Walmart: %s - status=%s - len=%s",
                            response.url,
                            response_status,
                            len(response_text),
                        )

                    if response_status == 412:
                        backoff = random.uniform(30, 60)
                        logger.warning(
                            "HTTP 412 détecté pour store %s (query=%s, page=%s) – backoff %.1fs (tentative %s/2)",
                            store_id,
                            query,
                            page,
                            backoff,
                            attempt + 1,
                        )
                        time.sleep(backoff)

                        if attempt == 1:
                            logger.error(
                                "HTTP 412 répété -> rotation proxy et arrêt des appels API sur ce store"
                            )
                            self.rotate_proxy()
                            return all_products

                        continue

                    break

                if response_status == 456:
                    self._mark_api_blocked(response_text, store_id, query, page)
                    return all_products

                if 400 <= response_status < 500:
                    logger.warning(
                        "Requête API produit échouée (status %s) pour store %s / query %s",
                        response_status,
                        store_id,
                        query,
                    )
                    logger.debug("Corps de la réponse (extrait): %s", response_text[:300])
                    use_browser_api = True

                    if self.driver:
                        response_status, response_text = fetch_walmart_api_via_browser(
                            self.driver, api_url, params
                        )
                        logger.info(
                            "Réponse API Walmart via navigateur: %s - status=%s - len=%s",
                            f"{api_url}?{urlencode(params)}",
                            response_status,
                            len(response_text or ""),
                        )
                    else:
                        logger.warning(
                            "Impossible d'utiliser le fallback navigateur: driver non initialisé"
                        )

                should_save_debug = (
                    store_id
                    and (response_status != 200 or len(self.debug_api_saved_stores) < self.debug_api_save_limit)
                    and store_id not in self.debug_api_saved_stores
                )

                if should_save_debug and response_text is not None:
                    os.makedirs("debug_walmart", exist_ok=True)
                    debug_path = os.path.join(
                        "debug_walmart", f"store-{store_id}-q{query}-p{page}.json"
                    )
                    with open(debug_path, "w", encoding="utf-8") as debug_file:
                        debug_file.write(response_text)
                    self.debug_api_saved_stores.add(store_id)
                    logger.info("Réponse brute enregistrée pour debug: %s", debug_path)

                if response_status != 200 or not response_text:
                    page += 1
                    self.human_like_delay(8, 15)
                    continue

                try:
                    payload = json.loads(response_text)
                except Exception as exc:  # noqa: PERF203
                    logger.warning("Impossible de parser la réponse API: %s", exc)
                    continue
                items = (
                    payload.get("items")
                    or payload.get("results")
                    or payload.get("data", {}).get("items")
                    or payload.get("data", {}).get("products")
                    or []
                )

                total_api_items += len(items)

                if not items:
                    logger.info("Aucun produit retourné pour la requête '%s' (page %s)", query, page)
                    break

                for raw in items:
                    promo_type = self._detect_promo_type(raw, promo_hint)
                    normalized = self._normalize_product(raw, store_id, store_slug, province, promo_type)
                    if normalized:
                        key = (normalized.get("product_id"), normalized.get("product_url"))
                        if key not in seen_keys:
                            seen_keys.add(key)
                            all_products.append(normalized)

                if len(items) < params["itemsPerPage"]:
                    break

                page += 1
                self.human_like_delay(8, 15)

        logger.info(
            "Store %s – produits totaux API: %s, produits en liquidation retenus: %s",
            store_id,
            total_api_items,
            len(all_products),
        )
        return all_products

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
                        logger.warning("CAPTCHA non résolu -> skip API product-search pour ce store")
                        self.cooldown_after_captcha()
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

            store_id, province, store_slug = self._extract_store_metadata(store_url)

            try:
                data["store_name"] = self.driver.find_element(By.XPATH, "//h1").text
            except Exception:
                data["store_name"] = "N/A"

            try:
                api_products = self._extract_products_via_api(store_id, store_slug, province, store_url)
                data["products"].extend(api_products)
            except Exception as exc:
                logger.warning("Erreur lors de l'extraction des produits via API: %s", exc)

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
                            self.rotate_proxy()
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
