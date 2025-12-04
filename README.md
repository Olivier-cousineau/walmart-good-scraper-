# Walmart Canada Scraper

Script Python pour scraper les magasins Walmart Canada avec Selenium et undetected-chromedriver. Le scraper gère la rotation de proxies, la résolution de CAPTCHA et exporte les résultats en CSV/JSON. Un workflow GitHub Actions permet de lancer le scraping manuellement et de récupérer les artefacts générés.

## Installation locale

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Utilisation en ligne de commande

```bash
python walmart_canada_scraper.py \
  --output-file walmart_canada_results.csv \
  --stores-per-province 5 \
  --proxies "http://user:pass@ip:port,http://user:pass@ip2:port2" \
  --captcha-api-key "VOTRE_CLE_2CAPTCHA" \
  --headless \
  --max-retries 3
```

Arguments disponibles :
- `--output-file` : nom du fichier CSV généré (le JSON est créé automatiquement).
- `--stores-per-province` : nombre de magasins par province (par défaut 5 pour des tests rapides).
- `--proxies` : liste de proxies résidentiels séparés par des virgules (optionnel).
- `--captcha-api-key` : clé API 2Captcha (optionnelle).
- `--headless` / `--no-headless` : active/désactive le mode headless (activé par défaut pour les workflows).
- `--max-retries` : nombre de tentatives par page en cas d'erreur.

## Workflow GitHub Actions manuel

Un workflow déclenchable manuellement (`Manual Walmart Canada scrape`) est disponible dans `.github/workflows/manual-scrape.yml`.

1. Aller dans l'onglet **Actions** du dépôt GitHub.
2. Sélectionner le workflow **Manual Walmart Canada scrape** puis cliquer sur **Run workflow**.
3. Renseigner au besoin :
   - `output_file` (nom du CSV),
   - `stores_per_province` (par défaut 5),
   - `proxies` (liste séparée par des virgules),
   - `captcha_api_key` (clé 2Captcha),
   - `headless` (true/false),
   - `max_retries` (nombre de tentatives par page).
4. Une fois terminé, récupérer les fichiers CSV/JSON dans les artefacts `walmart-canada-scrape`.

## Notes

- Le scraping complet des 402 magasins peut être long. Utilisez l'argument `--stores-per-province` pour limiter le volume lors des tests.
- L'utilisation de proxies résidentiels et d'une clé 2Captcha augmente la fiabilité face aux mesures anti-bot.
