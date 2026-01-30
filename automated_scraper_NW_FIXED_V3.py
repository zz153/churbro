#!/usr/bin/env python3
"""
New World Scraper - DUNEDIN FORCE + CLUB DEAL FIX

Preserves / enforces Dunedin store selection via:
  1) URL param: ?store=dunedin
  2) Geolocation spoof: (-45.8788, 170.5028) + geolocation permission
  3) NZ locale + Pacific/Auckland timezone
  4) Pre-load store forcing (cookies/localStorage attempts)

Also fixes Club Deal pricing:
  - Prefer badge unit price (handles 12.99 OR 12 99)
  - Exclude ALL /kg reference prices from discount candidate selection
"""

import asyncio
import pandas as pd
import logging
import argparse
import re
import random
import json
from datetime import datetime
from typing import List, Dict, Optional
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class NewWorldScraper:
    def __init__(self, headless: bool = True):
        self.base_url = "https://www.newworld.co.nz/shop/category/meat-poultry-and-seafood"
        self.headless = headless
        self.products: List[Dict] = []

        # Force Dunedin
        self.store_slug = "dunedin"
        self.geo = {"latitude": -45.8788, "longitude": 170.5028}  # Dunedin CBD

    async def _force_store_state(self, context, page):
        """
        Try to force Dunedin store selection BEFORE navigation.
        New World often remembers store via cookies/localStorage.
        """
        # 1) Best-effort: clear existing state so old "Timaru" doesn't win
        try:
            await context.clear_cookies()
        except Exception:
            pass

        # 2) Add some common “store” cookies (best effort; names may change)
        # If they don’t match, harmless.
        try:
            await context.add_cookies([
                {
                    "name": "store",
                    "value": self.store_slug,
                    "domain": ".newworld.co.nz",
                    "path": "/",
                },
                {
                    "name": "selectedStore",
                    "value": self.store_slug,
                    "domain": ".newworld.co.nz",
                    "path": "/",
                },
            ])
        except Exception:
            pass

        # 3) LocalStorage injection attempt (best effort)
        # Must run on a page at the domain. We'll load the homepage quickly first.
        try:
            await page.goto("https://www.newworld.co.nz/", wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(2)

            await page.evaluate(
                """(storeSlug) => {
                    try {
                        // common keys used by SPAs; if wrong, no harm
                        localStorage.setItem('store', storeSlug);
                        localStorage.setItem('selectedStore', storeSlug);
                        localStorage.setItem('preferredStore', storeSlug);

                        // sometimes stored as JSON
                        localStorage.setItem('storeSelection', JSON.stringify({ store: storeSlug }));

                        // sessionStorage too
                        sessionStorage.setItem('store', storeSlug);
                        sessionStorage.setItem('selectedStore', storeSlug);
                    } catch (e) {}
                }""",
                self.store_slug,
            )

            await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug(f"Store state injection skipped/failed (non-fatal): {e}")

    async def scrape_page(self, page, page_num: int) -> List[Dict]:
        products: List[Dict] = []
        try:
            selector = '[data-testid*="product"]'
            await asyncio.sleep(3)

            product_cards = await page.query_selector_all(selector)
            if not product_cards:
                logger.warning(f"  No products found on page {page_num}")
                return []

            logger.info(f"  Page {page_num}: Found {len(product_cards)} product cards")

            for i, card in enumerate(product_cards):
                try:
                    product = await self.parse_product_card(card)
                    if product and product.get("name"):
                        products.append(product)
                        if i < 5:
                            badges = []
                            if product.get("is_club_deal"):
                                badges.append("CLUB")
                            if product.get("is_super_saver"):
                                badges.append("SUPER")
                            badge_str = f" [{', '.join(badges)}]" if badges else ""
                            logger.info(
                                f"    ✓ {product.get('name', 'N/A')[:35]:35s} "
                                f"${product.get('sale_price', 0):.2f}{badge_str}"
                            )
                except Exception as e:
                    logger.debug(f"Error parsing card {i}: {e}")

            logger.info(f"  Extracted {len(products)} valid products")
        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {e}")

        return products

    async def parse_product_card(self, card) -> Optional[Dict]:
        try:
            full_text = await card.inner_text()

            name = await self.extract_name(card)
            if not name:
                return None

            is_club_deal = await self.is_club_deal(card, full_text)
            is_super_saver = await self.is_super_saver(card, full_text)

            price_data = await self.extract_all_prices(card, full_text, is_club_deal, is_super_saver, name)
            if not price_data:
                return None

            sku = await self.extract_sku(card)
            brand = await self.extract_brand(card)

            saving = price_data["original_price"] - price_data["sale_price"]

            return {
                "store": "newworld",
                "sku": sku,
                "name": name,
                "brand": brand,
                "sale_price": price_data["sale_price"],
                "original_price": price_data["original_price"],
                "price_per_kg": price_data.get("price_per_kg"),
                "unit_type": price_data.get("unit_type", "ea"),
                "saving": saving,
                "is_club_deal": is_club_deal,
                "is_super_saver": is_super_saver,
                "scraped_at": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.debug(f"Error parsing card: {e}")
            return None

    async def extract_name(self, card) -> Optional[str]:
        anchors = await card.query_selector_all("a")
        for anchor in anchors:
            text = (await anchor.inner_text()).strip()
            if text and len(text) > 5 and not text.startswith("$"):
                skip_words = ["add", "view", "cart", "more", "details", "shop", "buy"]
                if not any(w in text.lower() for w in skip_words):
                    return text

        for tag in ["h3", "h2", "h1", "h4"]:
            elem = await card.query_selector(tag)
            if elem:
                text = await elem.inner_text()
                if text and len(text.strip()) > 5:
                    return text.strip()

        name_elem = await card.query_selector('[class*="name"], [class*="Name"], [class*="title"], [class*="Title"]')
        if name_elem:
            return (await name_elem.inner_text()).strip()

        return None

    async def extract_all_prices(self, card, full_text: str, is_club_deal: bool, is_super_saver: bool, name: str = "") -> Optional[Dict]:
        def to_float(dollars: str, cents: str) -> float:
            return float(f"{int(dollars)}.{cents}")

        txt = full_text or ""

        ea_matches = re.findall(r"(?<!\d)(\d{1,3})[.,\s]*(\d{2})\s*(?:ea|each)\b", txt, re.IGNORECASE)
        ea_prices = [to_float(d, c) for d, c in ea_matches]

        kg_matches = re.findall(r"(?<!\d)(\d{1,3})[.,\s]*(\d{2})\s*kg\b", txt, re.IGNORECASE)
        kg_prices = [to_float(d, c) for d, c in kg_matches]

        per_kg_matches = re.findall(r"\$?\s*(\d{1,3})[.,\s]*(\d{2})\s*/\s*(?:1\s*)?kg\b", txt, re.IGNORECASE)
        per_kg_prices = [to_float(d, c) for d, c in per_kg_matches]
        per_kg_set = set(per_kg_prices)

        unit_type = None
        original_price = None
        sale_price = None

        if ea_prices:
            unit_type = "ea"
            original_price = ea_prices[-1]
            sale_price = original_price
        elif kg_prices:
            unit_type = "kg"
            original_price = kg_prices[-1]
            sale_price = original_price
        else:
            generic_matches = re.findall(r"(?<!\d)(\d{1,3})[.,\s]+(\d{2})(?!\d)", txt)
            generic_prices = [to_float(d, c) for d, c in generic_matches]
            generic_prices = [p for p in generic_prices if 0.5 <= p <= 500]
            if not generic_prices:
                return None
            unit_type = "ea"
            original_price = max(generic_prices)
            sale_price = original_price

        result = {"sale_price": sale_price, "original_price": original_price, "unit_type": unit_type}

        if per_kg_prices:
            result["price_per_kg"] = per_kg_prices[0]
        elif unit_type == "kg":
            result["price_per_kg"] = sale_price

        if is_club_deal:
            badge_price = None

            m = re.search(r"club\s*deal.*?\$?\s*(\d{1,3})\.(\d{2})", txt, re.IGNORECASE | re.DOTALL)
            if m:
                badge_price = to_float(m.group(1), m.group(2))

            if badge_price is None:
                m = re.search(r"club\s*deal.*?(?<!\d)(\d{1,3})[,\s]+(\d{2})(?!\d)", txt, re.IGNORECASE | re.DOTALL)
                if m:
                    badge_price = to_float(m.group(1), m.group(2))

            if badge_price and 0.5 <= badge_price <= 500 and badge_price < result["original_price"] - 0.01:
                result["sale_price"] = badge_price
            else:
                generic_matches = re.findall(r"(?<!\d)(\d{1,3})[.,\s]+(\d{2})(?!\d)", txt)
                all_prices = [to_float(d, c) for d, c in generic_matches]
                all_prices = [p for p in all_prices if 0.5 <= p <= 500]

                # KEY FIX: remove ALL /kg reference values
                if per_kg_set:
                    all_prices = [p for p in all_prices if p not in per_kg_set]

                discounted = [p for p in all_prices if p < result["original_price"] - 0.01]
                if discounted:
                    result["sale_price"] = min(discounted)

            # Guardrail: never let /kg ref become EA unit sale
            if result["unit_type"] == "ea" and result["sale_price"] in per_kg_set:
                logger.debug(f"Guardrail hit for '{name}'. Resetting sale_price to original.")
                result["sale_price"] = result["original_price"]

        if result["sale_price"] is None or result["original_price"] is None:
            return None
        if not (0.5 <= result["sale_price"] <= 500):
            return None

        return result

    async def is_club_deal(self, card, full_text: str) -> bool:
        text_lower = (full_text or "").lower()
        if "club deal" in text_lower or "clubdeal" in text_lower:
            return True

        images = await card.query_selector_all("img")
        for img in images:
            alt = await img.get_attribute("alt")
            if alt and "club" in alt.lower():
                return True
            src = await img.get_attribute("src")
            if src and "club" in src.lower():
                return True

        club_aria = await card.query_selector('[aria-label*="club" i], [aria-label*="Club" i]')
        if club_aria:
            return True

        club_data = await card.query_selector('[data-badge*="club" i], [data-promotion*="club" i]')
        if club_data:
            return True

        if re.search(r"\bclub\b", text_lower):
            return True

        return False

    async def is_super_saver(self, card, full_text: str) -> bool:
        text_lower = (full_text or "").lower()
        if "super saver" in text_lower or "supersaver" in text_lower:
            return True
        if "super" in text_lower and "saver" in text_lower:
            return True

        images = await card.query_selector_all("img")
        for img in images:
            alt = await img.get_attribute("alt")
            if alt:
                alt_lower = alt.lower()
                if "super" in alt_lower or "saver" in alt_lower:
                    return True
            src = await img.get_attribute("src")
            if src:
                src_lower = src.lower()
                if "super" in src_lower or "saver" in src_lower:
                    return True

        saver_aria = await card.query_selector('[aria-label*="super" i], [aria-label*="saver" i]')
        if saver_aria:
            return True

        saver_data = await card.query_selector('[data-badge*="super" i], [data-badge*="saver" i], [data-promotion*="super" i]')
        if saver_data:
            return True

        return False

    async def extract_sku(self, card) -> Optional[str]:
        for attr in ["data-stockcode", "data-sku", "data-product-id", "data-testid"]:
            val = await card.get_attribute(attr)
            if val:
                match = re.search(r"\d+", val)
                if match:
                    return match.group()
        return None

    async def extract_brand(self, card) -> Optional[str]:
        brand_elem = await card.query_selector('[class*="brand"], [class*="Brand"]')
        if brand_elem:
            return (await brand_elem.inner_text()).strip()
        return None

    async def scrape_all(self) -> List[Dict]:
        logger.info("🥩 Starting New World scrape (Dunedin forced + Club Deal fix)")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                ],
            )

            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                locale="en-NZ",
                timezone_id="Pacific/Auckland",
                geolocation=self.geo,
                permissions=["geolocation"],
            )

            await context.set_extra_http_headers({
                "Accept-Language": "en-NZ,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            })

            page = await context.new_page()

            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            """)

            # ✅ Force store selection state before scraping
            await self._force_store_state(context, page)

            page_num = 1
            max_pages = 100

            while page_num <= max_pages:
                url = f"{self.base_url}?store={self.store_slug}&pg={page_num}"
                logger.info(f"📄 Fetching page {page_num} from {self.store_slug.upper()}...")

                try:
                    if page_num > 1:
                        await asyncio.sleep(random.uniform(2, 4))

                    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(3 if page_num == 1 else 6)

                    # Verify store on first page
                    if page_num == 1:
                        content = (await page.content()).lower()
                        if "collect from new world timaru" in content:
                            logger.error("❌ STILL ON TIMARU. Store is being overridden by site state.")
                            logger.error("   Next step: we will need to capture the exact store cookie/localStorage key New World uses.")
                            logger.error("   (But scraper will continue for now.)")
                        elif "collect from new world dunedin" in content or "dunedin" in content:
                            logger.info("✅ Confirmed on Dunedin store!")

                    # Human-ish behavior
                    for _ in range(3):
                        await page.evaluate(f"window.scrollBy(0, {random.randint(300, 600)})")
                        await asyncio.sleep(random.uniform(0.5, 1.5))

                    await page.mouse.move(random.randint(100, 800), random.randint(100, 600))
                    await asyncio.sleep(0.5)

                    page_products = await self.scrape_page(page, page_num)
                    if not page_products:
                        logger.info("No products extracted, stopping")
                        break

                    self.products.extend(page_products)
                    page_num += 1

                except Exception as e:
                    logger.error(f"Error on page {page_num}: {e}")
                    break

            await browser.close()

        logger.info(f"✅ Scraped {len(self.products)} total products from {page_num - 1} pages")
        return self.products

    def save_to_csv(self, filename: Optional[str] = None) -> str:
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"newworld_specials_{timestamp}.csv"

        df = pd.DataFrame(self.products)

        column_order = [
            "store", "sku", "name", "brand",
            "sale_price", "original_price",
            "price_per_kg", "unit_type",
            "saving", "is_club_deal", "is_super_saver",
            "scraped_at",
        ]
        df = df[[c for c in column_order if c in df.columns]]

        df.to_csv(filename, index=False)
        logger.info(f"💾 Saved to {filename}")
        return filename


def main():
    parser = argparse.ArgumentParser(description="New World Scraper - Dunedin forced + Club Deal fix")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    scraper = NewWorldScraper(headless=args.headless)

    products = asyncio.run(scraper.scrape_all())
    if products:
        fn = scraper.save_to_csv()
        logger.info(f"✅ Success! {len(products)} products saved to {fn}")
    else:
        logger.warning("⚠️  No products found")


if __name__ == "__main__":
    main()

