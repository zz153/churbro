#!/usr/bin/env python3
"""
Mad Butcher Scraper (Dunedin) - FIXED SALE/ORIGINAL PRICE PARSING

What this fixes vs your current script:
- ✅ Correctly prioritises WooCommerce sale price inside <ins> (discounted)
- ✅ Correctly reads original/strikethrough price inside <del>
- ✅ Avoids overly-broad selectors (like plain `bdi`) that can grab the wrong price
- ✅ Keeps your Dunedin base URL and pagination as-is
- ✅ Keeps your name filtering logic

Important note:
- If Mad Butcher "Specials!" are rendered as an image/flyer (no text in HTML),
  no selector-based scraper can extract the special price. This script will still
  scrape all WooCommerce products correctly, and will capture specials IF they
  exist in the DOM as <del>/<ins> prices.
"""

import asyncio
import pandas as pd
import logging
import argparse
import re
import random
from datetime import datetime
from typing import List, Dict, Optional
from playwright.async_api import async_playwright

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MadButcherScraper:
    def __init__(self, headless: bool = True):
        self.base_url = "https://madbutcher.co.nz/dunedin/"
        self.headless = headless
        self.products: List[Dict] = []

    async def scrape_page(self, page, page_num: int) -> List[Dict]:
        """Scrape products from a single page"""
        products: List[Dict] = []

        try:
            await asyncio.sleep(3)

            # WooCommerce-ish selectors
            product_cards = await page.query_selector_all('.product, .type-product, li.product-type-simple, li.product')

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
                            on_sale = " [SALE]" if product.get("saving", 0) > 0 else ""
                            logger.info(
                                f"    ✓ {product.get('name', 'N/A')[:40]:40s} "
                                f"${product.get('sale_price', 0):.2f}{on_sale}"
                            )
                except Exception as e:
                    logger.debug(f"Error parsing card {i}: {e}")

            logger.info(f"  Extracted {len(products)} valid products")

        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {e}")

        return products

    async def parse_product_card(self, card) -> Optional[Dict]:
        """Extract data from a product card"""
        try:
            full_text = await card.inner_text()

            name = await self.extract_name(card)
            if not name:
                return None

            price_data = await self.extract_price(card, full_text)
            if not price_data:
                return None

            sku = await self.extract_sku(card)
            brand = "Mad Butcher"

            saving = price_data["original_price"] - price_data["sale_price"]

            return {
                "store": "madbutcher",
                "sku": sku,
                "name": name,
                "brand": brand,
                "sale_price": price_data["sale_price"],
                "original_price": price_data["original_price"],
                "price_per_kg": price_data.get("price_per_kg"),
                "unit_type": price_data.get("unit_type", "ea"),
                "saving": saving,
                "scraped_at": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.debug(f"Error parsing card: {e}")
            return None

    async def extract_name(self, card) -> Optional[str]:
        """Extract product name with filtering (keeps your logic)"""

        def is_valid_product_name(text: str) -> bool:
            if not text or len(text.strip()) < 5:
                return False

            text = text.strip()
            text_lower = text.lower()

            invalid_phrases = [
                "christmas hours",
                "opening hours",
                "holiday hours",
                "specials!",
                "specials",
                "closed",
                "contact us",
                "add to cart",
                "select options",
                "view details",
                "shop now",
            ]

            if any(phrase in text_lower for phrase in invalid_phrases):
                return False
            if text.startswith("$"):
                return False
            if len(text) < 12 and text.endswith("!"):
                return False

            return True

        # Strategy 1: WooCommerce product title
        name_elem = await card.query_selector(".woocommerce-loop-product__title, h2, h3, .product-title")
        if name_elem:
            text = await name_elem.inner_text()
            if is_valid_product_name(text):
                return text.strip()

        # Strategy 2: Product link
        link = await card.query_selector('a.woocommerce-LoopProduct-link, a[href*="product"]')
        if link:
            text = await link.inner_text()
            lines = [l.strip() for l in text.split("\n")]
            for line in lines:
                if is_valid_product_name(line):
                    return line

        # Strategy 3: Any anchor
        anchors = await card.query_selector_all("a")
        for anchor in anchors:
            text = (await anchor.inner_text()).strip()
            if is_valid_product_name(text):
                return text

        return None

    async def extract_price(self, card, full_text: str) -> Optional[Dict]:
        """
        Extract prices robustly:
        - Prefer sale price inside <ins>
        - Prefer original price inside <del>
        - If no sale structure, take normal displayed price
        """
        sale_price = None
        original_price = None
        unit_type = "ea"
        price_per_kg = None

        # 1) Sale price (ins)
        ins = await card.query_selector(".price ins bdi, .price ins .woocommerce-Price-amount")
        if ins:
            sale_price = self.parse_price_text(await ins.inner_text())

        # 2) Original price (del)
        dele = await card.query_selector(".price del bdi, .price del .woocommerce-Price-amount")
        if dele:
            original_price = self.parse_price_text(await dele.inner_text())

        # 3) Regular price if no ins
        if sale_price is None:
            amt = await card.query_selector(".price bdi, .price .woocommerce-Price-amount")
            if amt:
                sale_price = self.parse_price_text(await amt.inner_text())

        # 4) Fallback regex on full text
        if sale_price is None:
            matches = re.findall(r"\$\s*(\d+)\.(\d{2})", full_text)
            if matches:
                sale_price = float(f"{matches[0][0]}.{matches[0][1]}")

        if sale_price is None or sale_price < 1 or sale_price > 500:
            return None

        if original_price is None:
            original_price = sale_price

        # Unit detection
        txt = (full_text or "").lower()
        if "/kg" in txt or re.search(r"\bkg\b", txt):
            unit_type = "kg"
            price_per_kg = sale_price

        return {
            "sale_price": sale_price,
            "original_price": original_price,
            "unit_type": unit_type,
            "price_per_kg": price_per_kg,
        }

    def parse_price_text(self, text: str) -> Optional[float]:
        """Parse price from text like '$12.99' or '12.99'"""
        try:
            clean = re.sub(r"[^\d.]", "", text or "")
            if not clean:
                return None
            return float(clean)
        except Exception:
            return None

    async def extract_sku(self, card) -> Optional[str]:
        """Extract SKU/product ID"""
        for attr in ["data-product-id", "data-product_id", "data-id"]:
            val = await card.get_attribute(attr)
            if val:
                return val

        class_attr = await card.get_attribute("class")
        if class_attr:
            match = re.search(r"post-(\d+)", class_attr)
            if match:
                return match.group(1)

        return None

    async def scrape_all(self) -> List[Dict]:
        """Scrape all pages"""
        logger.info("🥩 Starting Mad Butcher scrape (FIXED sale parsing)")

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
                geolocation={"latitude": -45.8788, "longitude": 170.5028},
                permissions=["geolocation"],
            )

            page = await context.new_page()

            page_num = 1
            max_pages = 30

            while page_num <= max_pages:
                url = f"{self.base_url}?product-page={page_num}"
                logger.info(f"📄 Fetching page {page_num}...")

                try:
                    if page_num > 1:
                        await asyncio.sleep(random.uniform(2, 4))

                    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(3)

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
        """Save products to CSV"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"madbutcher_products_{timestamp}.csv"

        df = pd.DataFrame(self.products)

        column_order = [
            "store",
            "sku",
            "name",
            "brand",
            "sale_price",
            "original_price",
            "price_per_kg",
            "unit_type",
            "saving",
            "scraped_at",
        ]
        df = df[[c for c in column_order if c in df.columns]]

        df.to_csv(filename, index=False)
        logger.info(f"💾 Saved to {filename}")

        if len(df) > 0:
            logger.info("📊 Stats:")
            logger.info(f"   Total products: {len(df)}")
            logger.info(f"   Price range: ${df['sale_price'].min():.2f} - ${df['sale_price'].max():.2f}")
            logger.info(f"   Avg price: ${df['sale_price'].mean():.2f}")
            on_sale = df[df["saving"] > 0]
            if len(on_sale) > 0:
                logger.info(f"   On sale: {len(on_sale)} products")
                logger.info(f"   Avg saving: ${on_sale['saving'].mean():.2f}")

        return filename


def main():
    parser = argparse.ArgumentParser(description="Mad Butcher Scraper (FIXED sale parsing)")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    scraper = MadButcherScraper(headless=args.headless)

    try:
        products = asyncio.run(scraper.scrape_all())

        if products:
            filename = scraper.save_to_csv()
            logger.info(f"✅ Success! {len(products)} products saved to {filename}")
        else:
            logger.warning("⚠️  No products found")

    except Exception as e:
        logger.error(f"❌ Scraper failed: {e}")
        raise


if __name__ == "__main__":
    main()

