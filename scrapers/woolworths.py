#!/usr/bin/env python3
"""
Woolworths Scraper V4 - Dunedin Enforced (All-in-One)
======================================================
SAME browser session: sets Dunedin FIRST, then scrapes.

Flow (every run):
  1. Open woolworths.co.nz
  2. Click "Change location"
  3. Click "Change address >"
  4. Type "Dunedin" → select "Dunedin CBD, Dunedin"
  5. Click "Save and Continue Shopping"
  6. Navigate to /shop/browse/meat-poultry
  7. Verify banner says "Dunedin CBD"
  8. Scrape all pages

USAGE:
    python3 automated_scraper_WW_V4.py
    python3 automated_scraper_WW_V4.py --debug
"""

import asyncio
import pandas as pd
import logging
import argparse
import re
import random
from datetime import datetime
from typing import List, Dict
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class WoolworthsScraper:
    def __init__(self):
        self.categories = [
            # Fresh meat & poultry (existing)
            {'url': 'https://www.woolworths.co.nz/shop/browse/meat-poultry', 'label': 'fresh'},
            # Frozen meat subcategories
            {'url': 'https://www.woolworths.co.nz/shop/browse/frozen/frozen-meat/frozen-chicken-poultry', 'label': 'frozen'},
            {'url': 'https://www.woolworths.co.nz/shop/browse/frozen/frozen-meat/frozen-burgers', 'label': 'frozen'},
            {'url': 'https://www.woolworths.co.nz/shop/browse/frozen/frozen-meat/frozen-beef-lamb-pork', 'label': 'frozen'},
            # Frozen meat alternatives
            {'url': 'https://www.woolworths.co.nz/shop/browse/frozen/frozen-meat-alternatives', 'label': 'frozen'},
            # Frozen seafood subcategories
            {'url': 'https://www.woolworths.co.nz/shop/browse/frozen/frozen-seafood/frozen-fish', 'label': 'frozen'},
            {'url': 'https://www.woolworths.co.nz/shop/browse/frozen/frozen-seafood/frozen-marinara', 'label': 'frozen'},
            {'url': 'https://www.woolworths.co.nz/shop/browse/frozen/frozen-seafood/frozen-prawns-squid', 'label': 'frozen'},
        ]
        self.base_url = self.categories[0]['url']
        self.products = []

    async def run(self):
        """Single browser session: set location → scrape."""

        logger.info("🥩 Woolworths V4 — Dunedin Enforced")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,  # MUST be visible — Woolworths blocks headless
                args=['--disable-blink-features=AutomationControlled', '--disable-dev-shm-usage']
            )

            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='en-NZ',
                timezone_id='Pacific/Auckland',
                geolocation={'latitude': -45.8788, 'longitude': 170.5028},
                permissions=['geolocation'],
            )

            await context.clear_cookies()

            await context.set_extra_http_headers({
                'Accept-Language': 'en-NZ,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            })

            page = await context.new_page()
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            """)

            # ============================
            # PHASE 1: SET DUNEDIN LOCATION
            # ============================
            dunedin_set = await self.set_dunedin(page)

            if not dunedin_set:
                logger.error("❌ Could not set Dunedin. Aborting.")
                await browser.close()
                return []

            # ============================
            # PHASE 2: SCRAPE PRODUCTS
            # ============================
            await self.scrape_all_pages(page)

            await browser.close()

        logger.info(f"✅ Total: {len(self.products)} products scraped")
        return self.products

    # ==================================================================
    # PHASE 1: DUNEDIN LOCATION (same page/session as scraping)
    # ==================================================================

    async def set_dunedin(self, page) -> bool:
        """
        Walk through the exact Woolworths location change flow:
        Screenshot 1: Homepage → "Change location"
        Screenshot 2: bookatimeslot → "Change address >"
        Screenshot 3: Modal with "Start typing your suburb/town"
        Screenshot 4: Type "Dunedin" → dropdown appears
        Screenshot 5: Select "Dunedin CBD, Dunedin" → "Save and Continue Shopping"
        Screenshot 6: Confirms "Deliver to: Dunedin CBD"
        """

        # --- STEP 1: Load homepage ---
        logger.info("🔵 STEP 1: Loading woolworths.co.nz...")
        await page.goto("https://www.woolworths.co.nz/", wait_until='domcontentloaded', timeout=60000)
        await asyncio.sleep(5)

        # Check if already Dunedin
        content = (await page.content()).lower()
        if "dunedin" in content and "glenfield" not in content:
            logger.info("✅ Already set to Dunedin! Skipping location change.")
            return True

        # --- STEP 2: Click "Change location" link in the banner ---
        logger.info("🔵 STEP 2: Clicking 'Change location'...")
        clicked = False
        for selector in [
            'text="Change location"',
            'a:has-text("Change location")',
            'button:has-text("Change location")',
        ]:
            try:
                await page.click(selector, timeout=8000)
                clicked = True
                logger.info(f"   ✅ Clicked via: {selector}")
                break
            except Exception:
                continue

        if not clicked:
            # Fallback: click "Pick up or delivery?"
            try:
                await page.click('text="Pick up or delivery?"', timeout=8000)
                clicked = True
                logger.info("   ✅ Clicked 'Pick up or delivery?'")
            except Exception:
                pass

        if not clicked:
            logger.error("   ❌ Cannot find 'Change location'. Manual fallback.")
            print("\n⚠️  Please click 'Change location' manually in the browser.")
            input("   Press ENTER when you're on the bookatimeslot page... ")

        await asyncio.sleep(4)

        # --- STEP 3: We should now be on woolworths.co.nz/bookatimeslot ---
        # Click "Change address >"
        logger.info("🔵 STEP 3: Clicking 'Change address >'...")
        clicked = False
        for selector in [
            'text="Change address"',
            'a:has-text("Change address")',
            'button:has-text("Change address")',
        ]:
            try:
                await page.click(selector, timeout=8000)
                clicked = True
                logger.info(f"   ✅ Clicked via: {selector}")
                break
            except Exception:
                continue

        if not clicked:
            logger.warning("   ⚠️  Cannot find 'Change address'. Trying direct navigation...")
            # Maybe we're already on the modal, or we need to look for "Delivery" first
            try:
                # Try clicking "Delivery" option first
                await page.click('text="Delivery"', timeout=5000)
                await asyncio.sleep(2)
                for selector in ['text="Change address"', 'a:has-text("Change address")']:
                    try:
                        await page.click(selector, timeout=5000)
                        clicked = True
                        break
                    except Exception:
                        continue
            except Exception:
                pass

        if not clicked:
            logger.error("   ❌ Cannot find 'Change address'. Manual fallback.")
            print("\n⚠️  Please click 'Change address >' manually in the browser.")
            input("   Press ENTER when the delivery zone modal appears... ")

        await asyncio.sleep(3)

        # --- STEP 4: Type "Dunedin" in the delivery zone input ---
        logger.info("🔵 STEP 4: Typing 'Dunedin' in delivery zone...")
        input_elem = None

        for sel in [
            'input[placeholder*="Start typing your suburb"]',
            'input[placeholder*="Start typing"]',
            'input[placeholder*="suburb"]',
            'input[placeholder*="town"]',
        ]:
            try:
                input_elem = await page.wait_for_selector(sel, timeout=5000)
                if input_elem:
                    break
            except Exception:
                continue

        if not input_elem:
            # Fallback: any visible text input
            try:
                inputs = await page.query_selector_all('input[type="text"], input:not([type])')
                for inp in inputs:
                    if await inp.is_visible():
                        input_elem = inp
                        break
            except Exception:
                pass

        if not input_elem:
            logger.error("   ❌ Cannot find delivery zone input. Manual fallback.")
            print("\n⚠️  Please type 'Dunedin' and select 'Dunedin CBD' manually.")
            input("   Press ENTER after selecting Dunedin CBD and clicking Save... ")
            return await self.verify_dunedin(page)

        await input_elem.click()
        await asyncio.sleep(0.5)
        await input_elem.fill('')
        await asyncio.sleep(0.3)
        await input_elem.type('Dunedin', delay=150)
        logger.info("   ✅ Typed 'Dunedin'")
        await asyncio.sleep(3)

        # --- STEP 5: Select "Dunedin CBD, Dunedin" from dropdown ---
        logger.info("🔵 STEP 5: Selecting 'Dunedin CBD, Dunedin'...")
        selected = False
        for selector in [
            'text="Dunedin CBD, Dunedin"',
            'li:has-text("Dunedin CBD")',
            'div:has-text("Dunedin CBD, Dunedin")',
        ]:
            try:
                await page.click(selector, timeout=8000)
                selected = True
                logger.info(f"   ✅ Selected via: {selector}")
                break
            except Exception:
                continue

        if not selected:
            # Fallback: scan all elements
            try:
                els = await page.query_selector_all('li, div, span, button, a')
                for el in els:
                    try:
                        text = await el.inner_text()
                        if 'dunedin cbd' in text.lower() and await el.is_visible():
                            await el.click()
                            selected = True
                            logger.info(f"   ✅ Selected via scan: {text.strip()[:40]}")
                            break
                    except Exception:
                        continue
            except Exception:
                pass

        if not selected:
            logger.error("   ❌ Cannot find 'Dunedin CBD'. Manual fallback.")
            print("\n⚠️  Please select 'Dunedin CBD, Dunedin' manually.")
            input("   Press ENTER after selecting... ")

        await asyncio.sleep(2)

        # --- STEP 6: Click "Save and Continue Shopping" ---
        logger.info("🔵 STEP 6: Clicking 'Save and Continue Shopping'...")
        saved = False
        for selector in [
            'text="Save and Continue Shopping"',
            'button:has-text("Save and Continue Shopping")',
            'button:has-text("Save and Continue")',
            'button:has-text("Save")',
        ]:
            try:
                await page.click(selector, timeout=8000)
                saved = True
                logger.info(f"   ✅ Clicked via: {selector}")
                break
            except Exception:
                continue

        if not saved:
            logger.error("   ❌ Cannot find Save button. Manual fallback.")
            print("\n⚠️  Please click 'Save and Continue Shopping' manually.")
            input("   Press ENTER after clicking Save... ")

        await asyncio.sleep(5)

        # --- STEP 7: Navigate to meat-poultry and verify ---
        logger.info("🔵 STEP 7: Going to meat-poultry page and verifying...")
        await page.goto(self.base_url, wait_until='domcontentloaded', timeout=60000)
        await asyncio.sleep(5)

        return await self.verify_dunedin(page)

    async def verify_dunedin(self, page) -> bool:
        """Check banner says Dunedin, not Glenfield."""
        content = (await page.content()).lower()
        if "glenfield" in content:
            logger.error("❌ STILL SHOWING GLENFIELD after location change!")
            print("\n❌ Woolworths is still showing Glenfield.")
            print("   Please change location to Dunedin CBD manually in the browser.")
            input("   Press ENTER when the banner says 'Dunedin CBD area'... ")
            # Re-check
            content = (await page.content()).lower()
            if "glenfield" in content:
                logger.error("❌ Still Glenfield. Cannot proceed.")
                return False
        if "dunedin" in content:
            logger.info("✅ DUNEDIN CBD CONFIRMED! Ready to scrape.")
            return True
        logger.warning("⚠️  Neither Glenfield nor Dunedin found. Proceeding anyway.")
        return True

    # ==================================================================
    # PHASE 2: SCRAPE PRODUCTS (same page/session)
    # ==================================================================

    async def scrape_all_pages(self, page):
        """Scrape all categories of meat/frozen. Page is ALREADY on the right URL."""

        for category in self.categories:
            base_url = category['url']
            label = category['label']
            cat_name = base_url.split('/')[-1]
            logger.info(f"\n📦 Scraping: {cat_name} [{label}]")

            # Navigate to category first page
            await page.goto(f"{base_url}?page=1&inStockProductsOnly=false", wait_until='domcontentloaded', timeout=60000)
            await asyncio.sleep(5)

            # Verify still on Dunedin
            page_content = (await page.content()).lower()
            if "glenfield" in page_content:
                logger.error(f"❌ Glenfield detected on {cat_name}! Skipping.")
                continue

            page_num = 1
            max_pages = 100

            while page_num <= max_pages:
                if page_num > 1:
                    url = f"{base_url}?page={page_num}&inStockProductsOnly=false"
                    logger.info(f"📄 Fetching page {page_num}...")
                    await asyncio.sleep(random.uniform(2, 4))
                    await page.goto(url, wait_until='domcontentloaded', timeout=45000)

                    page_content = (await page.content()).lower()
                    if "glenfield" in page_content:
                        logger.error(f"❌ Page {page_num}: Glenfield detected! Stopping.")
                        break
                else:
                    logger.info(f"📄 Scraping page 1...")

                await asyncio.sleep(4 if page_num == 1 else 6)

                for _ in range(3):
                    await page.evaluate(f'window.scrollBy(0, {random.randint(300, 600)})')
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                await page.mouse.move(random.randint(100, 800), random.randint(100, 600))
                await asyncio.sleep(0.5)

                page_products = await self.scrape_page(page, page_num)

                if not page_products:
                    logger.info(f"  No products on page {page_num}, moving to next category")
                    break

                for product in page_products:
                    product['category'] = label

                self.products.extend(page_products)
                page_num += 1

        logger.info(f"✅ Scraped {len(self.products)} total products")

    async def scrape_page(self, page, page_num: int) -> List[Dict]:
        products = []
        try:
            await asyncio.sleep(3)
            cards = await page.query_selector_all('.product-entry')
            if not cards:
                logger.warning(f"  No products on page {page_num}")
                return []
            logger.info(f"  Page {page_num}: {len(cards)} cards")
            for i, card in enumerate(cards):
                try:
                    product = await self.parse_card(card)
                    if product:
                        products.append(product)
                        if i < 5:
                            logger.info(f"    ✓ {product['name'][:40]:40s} ${product['sale_price']:.2f}")
                except Exception as e:
                    logger.debug(f"Card {i} error: {e}")
            logger.info(f"  Extracted {len(products)} products")
        except Exception as e:
            logger.error(f"Page {page_num} error: {e}")
        return products

    async def parse_card(self, card) -> Dict:
        name = await self.get_name(card)
        if not name:
            return None
        sale_price = await self.get_price(card)
        if not sale_price:
            return None
        original_price = await self.get_was_price(card)
        if not original_price:
            original_price = sale_price
        sku = await self.get_sku(card)
        brand = await self.get_brand(card, name)
        saving = round(original_price - sale_price, 2) if original_price > sale_price else 0
        return {
            'store': 'woolworths', 'sku': sku, 'name': name, 'brand': brand,
            'sale_price': sale_price, 'original_price': original_price,
            'saving': saving, 'scraped_at': datetime.now().isoformat()
        }

    async def get_name(self, card) -> str:
        def ok(t):
            if not t or len(t.strip()) < 10:
                return False
            t = t.strip()
            if t.startswith('$'):
                return False
            nums = re.sub(r'[^\d]', '', t)
            if nums and len(nums) / len(t) > 0.5:
                return False
            if not re.search(r'[a-zA-Z]{3,}', t):
                return False
            return True

        for tag in ['h3', 'h2', 'h1', 'h4']:
            el = await card.query_selector(tag)
            if el:
                t = await el.inner_text()
                if ok(t):
                    return t.strip()
        link = await card.query_selector('a[class*="product"]')
        if link:
            for line in (await link.inner_text()).split('\n'):
                if ok(line):
                    return line.strip()
        for attr in ['aria-label', 'title']:
            v = await card.get_attribute(attr)
            if v and ok(v):
                return v.strip()
        return None

    async def get_price(self, card) -> float:
        d_el = await card.query_selector('.price-dollars, [class*="price-dollar"]')
        c_el = await card.query_selector('.price-cents, [class*="price-cent"]')
        if d_el and c_el:
            try:
                d = re.sub(r'[^\d]', '', await d_el.inner_text())
                c = re.sub(r'[^\d]', '', await c_el.inner_text())
                if d:
                    p = float(f"{d}.{c if c else '00'}")
                    if 1 < p < 500:
                        return p
            except Exception:
                pass
        for sel in ['[class*="price-dollar"]', '[class*="product-price"]', '[class*="current-price"]', '.price']:
            el = await card.query_selector(sel)
            if el:
                m = re.search(r'\$?(\d+)[\s.]?(\d{2})?', await el.inner_text())
                if m:
                    p = float(f"{m.group(1)}.{m.group(2) or '00'}")
                    ft = await card.inner_text()
                    if f"for ${m.group(1)}" not in ft and 1 < p < 500:
                        return p
        full = await card.inner_text()
        matches = re.findall(r'\$(\d+)\.(\d{2})', full)
        if matches:
            prices = []
            for m in matches:
                p = float(f"{m[0]}.{m[1]}")
                if f"for ${m[0]}.{m[1]}" not in full and 1 < p < 100:
                    prices.append(p)
            if prices:
                return min(prices)
        return None

    async def get_was_price(self, card) -> float:
        full = await card.inner_text()
        m = re.search(r'was\s+\$(\d+)\.(\d{2})', full, re.IGNORECASE)
        if m:
            return float(f"{m.group(1)}.{m.group(2)}")
        for sel in ['[class*="was"]', '[class*="crossed"]', '[class*="original"]', 'del', 's']:
            el = await card.query_selector(sel)
            if el:
                m = re.search(r'\$?(\d+)\.(\d{2})', await el.inner_text())
                if m:
                    return float(f"{m.group(1)}.{m.group(2)}")
        return None

    async def get_sku(self, card) -> str:
        for attr in ['data-stockcode', 'data-sku', 'data-product-id', 'stockcode']:
            v = await card.get_attribute(attr)
            if v:
                m = re.search(r'\d+', v)
                if m:
                    return m.group()
        href = await card.get_attribute('href')
        if not href:
            a = await card.query_selector('a')
            if a:
                href = await a.get_attribute('href')
        if href:
            m = re.search(r'/product/(\d+)', href)
            if m:
                return m.group(1)
        return None

    async def get_brand(self, card, name: str) -> str:
        el = await card.query_selector('[class*="brand"], [class*="Brand"]')
        if el:
            return await el.inner_text()
        if name and 'woolworths' in name.lower():
            return 'Woolworths'
        return None

    def save_to_csv(self, filename=None):
        if not filename:
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'woolworths_dunedin_{ts}.csv'
        df = pd.DataFrame(self.products)
        cols = ['store', 'sku', 'name', 'brand', 'sale_price', 'original_price', 'saving', 'scraped_at']
        cols = [c for c in cols if c in df.columns]
        df[cols].to_csv(filename, index=False)
        logger.info(f"💾 Saved to {filename}")
        if len(df) > 0:
            logger.info(f"📊 {len(df)} products | ${df['sale_price'].min():.2f}-${df['sale_price'].max():.2f} | avg ${df['sale_price'].mean():.2f}")
        return filename


def main():
    parser = argparse.ArgumentParser(description='Woolworths V4 - Dunedin All-in-One')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    scraper = WoolworthsScraper()
    products = asyncio.run(scraper.run())

    if products:
        filename = scraper.save_to_csv()
        logger.info(f"✅ Done! {len(products)} products → {filename}")
    else:
        logger.warning("⚠️  No products found")


if __name__ == '__main__':
    main()
