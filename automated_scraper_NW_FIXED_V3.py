#!/usr/bin/env python3
"""
New World Scraper - FIXED VERSION V4
- FIXED: Club Deal price now uses min() not max() (line 256 bug)
- PRIORITIZES "ea" prices (NEVER uses kg prices as main price!)
- Correctly extracts Club Deal discounted prices
- Simple, robust price extraction logic
- Shows actual savings for products on special
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

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NewWorldScraper:
    def __init__(self, headless=True):
        self.base_url = "https://www.newworld.co.nz/shop/category/meat-poultry-and-seafood"
        self.headless = headless
        self.products = []
        
    async def scrape_page(self, page, page_num: int) -> List[Dict]:
        """Scrape products from a single page"""
        
        products = []
        
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
                    if product and product.get('name'):
                        products.append(product)
                        if i < 5:  # Debug first 5
                            badges = []
                            if product.get('is_club_deal'):
                                badges.append('CLUB')
                            if product.get('is_super_saver'):
                                badges.append('SUPER')
                            badge_str = f" [{', '.join(badges)}]" if badges else ""
                            logger.info(f"    ✓ {product.get('name', 'N/A')[:35]:35s} ${product.get('sale_price', 0):.2f}{badge_str}")
                        
                        # Extra debug for first product to see badge detection
                        if i == 0:
                            card_text = await card.inner_text()
                            logger.debug(f"    First card text sample: {card_text[:200]}")
                            
                            # Check for images
                            images = await card.query_selector_all('img')
                            logger.debug(f"    Found {len(images)} images in first card")
                            for img in images[:3]:
                                alt = await img.get_attribute('alt')
                                src = await img.get_attribute('src')
                                logger.debug(f"      Image: alt='{alt}', src contains: {src[-50:] if src else 'None'}")
                            
                except Exception as e:
                    logger.debug(f"Error parsing card {i}: {e}")
            
            logger.info(f"  Extracted {len(products)} valid products")
                    
        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {e}")
        
        return products
    
    async def parse_product_card(self, card) -> Dict:
        """Extract data from a product card"""
        
        try:
            # Get all text for debugging
            full_text = await card.inner_text()
            
            # 1. GET PRODUCT NAME
            name = await self.extract_name(card)
            if not name:
                return None
            
            # 2. DETECT SPECIAL DEAL BADGES FIRST
            is_club_deal = await self.is_club_deal(card, full_text)
            is_super_saver = await self.is_super_saver(card, full_text)
            
            # 3. GET ALL PRICES (unit price, Club Deal price, per kg)
            price_data = await self.extract_all_prices(card, full_text, is_club_deal, is_super_saver, name)
            if not price_data:
                return None
            
            # Debug badge detection
            if is_club_deal or is_super_saver:
                logger.debug(f"    Badge detected for '{name[:30]}': club={is_club_deal}, super={is_super_saver}, prices={price_data}")
            
            # 4. GET SKU
            sku = await self.extract_sku(card)
            
            # 5. GET BRAND
            brand = await self.extract_brand(card)
            
            # Calculate savings
            saving = price_data['original_price'] - price_data['sale_price']
            
            return {
                'store': 'newworld',
                'sku': sku,
                'name': name,
                'brand': brand,
                'sale_price': price_data['sale_price'],
                'original_price': price_data['original_price'],
                'price_per_kg': price_data.get('price_per_kg'),
                'unit_type': price_data.get('unit_type', 'ea'),
                'saving': saving,
                'is_club_deal': is_club_deal,
                'is_super_saver': is_super_saver,
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.debug(f"Error parsing card: {e}")
            return None
    
    async def extract_name(self, card) -> str:
        """Extract product name"""
        
        # Strategy 1: Look for anchor tags (product links)
        anchors = await card.query_selector_all('a')
        for anchor in anchors:
            text = await anchor.inner_text()
            text = text.strip()
            
            if text and len(text) > 5 and not text.startswith('$'):
                skip_words = ['add', 'view', 'cart', 'more', 'details', 'shop', 'buy']
                if not any(word in text.lower() for word in skip_words):
                    return text
        
        # Strategy 2: Look for headings
        for tag in ['h3', 'h2', 'h1', 'h4']:
            elem = await card.query_selector(tag)
            if elem:
                text = await elem.inner_text()
                if text and len(text.strip()) > 5:
                    return text.strip()
        
        # Strategy 3: Look for name/title classes
        name_elem = await card.query_selector('[class*="name"], [class*="Name"], [class*="title"], [class*="Title"]')
        if name_elem:
            return await name_elem.inner_text()
        
        return None
    
    async def extract_all_prices(self, card, full_text: str, is_club_deal: bool, is_super_saver: bool, name: str = "") -> Dict:
        """
        Extract prices from a New World product card.

        We treat the *unit price* as the main price shown beside the unit label:
          - "ea" / "each" products: e.g. "22 39 ea"
          - "kg" products:        e.g. "12 89 kg"

        We also capture reference prices like "$27.99/1kg" as price_per_kg.

        For Club Deals:
          - The Club Deal badge usually contains the discounted unit price (e.g. "18 79")
          - The black unit price is the "original" (non-club) price shown on the card (e.g. "22 39 ea")
        """

        text_lower = (full_text or "").lower()

        def to_float(dollars: str, cents: str) -> float:
            return float(f"{int(dollars)}.{cents}")

        # 1) Unit price (ea)
        ea_matches = re.findall(r'(?<!\d)(\d{1,3})[.,\s]*(\d{2})\s*(?:ea|each)\b', full_text, re.IGNORECASE)
        ea_prices = [to_float(d, c) for d, c in ea_matches]

        # 2) Unit price (kg) — these are products sold by weight
        kg_matches = re.findall(r'(?<!\d)(\d{1,3})[.,\s]*(\d{2})\s*kg\b', full_text, re.IGNORECASE)
        kg_prices = [to_float(d, c) for d, c in kg_matches]

        # 3) Reference prices per kg (usually grey "$xx.xx/1kg" or "$xx.xx/kg")
        per_kg_matches = re.findall(r'\$?\s*(\d{1,3})[.,\s]*(\d{2})\s*/\s*(?:1\s*)?kg\b', full_text, re.IGNORECASE)
        per_kg_prices = [to_float(d, c) for d, c in per_kg_matches]

        # 4) Decide unit_type + base/original price
        # Prefer "ea" prices; if none, fall back to "kg".
        unit_type = None
        original_price = None
        sale_price = None

        if ea_prices:
            unit_type = "ea"
            # If multiple ea prices appear, take the most plausible one (usually the last one in the card text is the main price)
            original_price = ea_prices[-1]
            sale_price = original_price
        elif kg_prices:
            unit_type = "kg"
            original_price = kg_prices[-1]
            sale_price = original_price
        else:
            # Fallback: capture split price tokens even when unit labels are missing
            # e.g. "22\n39" (avoid matching long numbers)
            generic_matches = re.findall(r'(?<!\d)(\d{1,3})[.,\s]+(\d{2})(?!\d)', full_text)
            generic_prices = [to_float(d, c) for d, c in generic_matches]
            generic_prices = [p for p in generic_prices if 0.5 <= p <= 500]
            if not generic_prices:
                return None
            # Pick a reasonable candidate: highest (most likely the main unit price), not a per-kg ref
            unit_type = "ea"
            original_price = max(generic_prices)
            sale_price = original_price

        result = {
            "sale_price": sale_price,
            "original_price": original_price,
            "unit_type": unit_type,
        }

        # 5) Store reference price per kg (if present)
        if per_kg_prices:
            # Typically the first "/kg" ref is the main reference
            result["price_per_kg"] = per_kg_prices[0]
        elif unit_type == "kg":
            # If sold by kg, unit price is itself per kg
            result["price_per_kg"] = sale_price

        # 6) Club Deal discounted price
        if is_club_deal:
            # Try to pull the badge price directly (most reliable).
            # Works when the price is rendered like "18\n79" without a decimal.
            m = re.search(r'club\s*deal\s*(\d{1,3})[.,\s]+(\d{2})(?!\d)', full_text, re.IGNORECASE)
            badge_price = to_float(m.group(1), m.group(2)) if m else None

            if badge_price and 0.5 <= badge_price <= 500:
                # Badge should be a discount vs original price; if not, keep original.
                if badge_price < result["original_price"]:
                    result["sale_price"] = badge_price
            else:
                # Fallback heuristic: choose the smallest non-ref price below original.
                generic_matches = re.findall(r'(?<!\d)(\d{1,3})[.,\s]+(\d{2})(?!\d)', full_text)
                all_prices = [to_float(d, c) for d, c in generic_matches]
                all_prices = [p for p in all_prices if 0.5 <= p <= 500]

                # remove obvious per-kg refs if they exist (e.g. "$23.49/1kg")
                # NOTE: we already capture per_kg separately, so exclude exact matches.
                if "price_per_kg" in result:
                    all_prices = [p for p in all_prices if abs(p - result["price_per_kg"]) > 1e-6]

                discounted = [p for p in all_prices if p < result["original_price"] - 0.01]
                if discounted:
                    result["sale_price"] = min(discounted)

        # 7) Super Saver: usually just a badge, often no "original" shown.
        # Keep sale==original unless you later add explicit strike-through parsing.

        # Final sanity checks
        if result["sale_price"] is None or result["original_price"] is None:
            return None

        if not (0.5 <= result["sale_price"] <= 500):
            return None

        return result
    async def is_club_deal(self, card, full_text: str) -> bool:
        """Check if product has Club Deal badge"""
        
        # Strategy 1: Look for "Club Deal" or just "Club" in text (case insensitive)
        text_lower = full_text.lower()
        if 'club deal' in text_lower or 'clubdeal' in text_lower:
            return True
        
        # Strategy 2: Check for images with "club" in alt text
        images = await card.query_selector_all('img')
        for img in images:
            alt = await img.get_attribute('alt')
            if alt and 'club' in alt.lower():
                return True
            
            src = await img.get_attribute('src')
            if src and 'club' in src.lower():
                return True
        
        # Strategy 3: Look for any element with "club" in aria-label
        club_aria = await card.query_selector('[aria-label*="club" i], [aria-label*="Club" i]')
        if club_aria:
            return True
        
        # Strategy 4: Check data attributes
        club_data = await card.query_selector('[data-badge*="club" i], [data-promotion*="club" i]')
        if club_data:
            return True
        
        # Strategy 5: Look for specific badge wrapper classes
        badge_selectors = [
            '[class*="badge"][class*="club" i]',
            '[class*="promo"][class*="club" i]',
            '[class*="label"][class*="club" i]',
        ]
        
        for selector in badge_selectors:
            try:
                elem = await card.query_selector(selector)
                if elem:
                    return True
            except:
                pass
        
        # Strategy 6: Check for standalone "Club" text (common in badges)
        if text_lower.count('club') > 0:
            # Make sure it's not part of a longer word
            if re.search(r'\bclub\b', text_lower):
                return True
        
        return False
    
    async def is_super_saver(self, card, full_text: str) -> bool:
        """Check if product has Super Saver badge"""
        
        # Strategy 1: Look for "Super Saver" or variations in text
        text_lower = full_text.lower()
        if 'super saver' in text_lower or 'supersaver' in text_lower:
            return True
        
        # Also check for just "super" near "saver"
        if 'super' in text_lower and 'saver' in text_lower:
            return True
        
        # Strategy 2: Check for images with "super" or "saver" in alt text
        images = await card.query_selector_all('img')
        for img in images:
            alt = await img.get_attribute('alt')
            if alt:
                alt_lower = alt.lower()
                if 'super' in alt_lower or 'saver' in alt_lower:
                    return True
            
            src = await img.get_attribute('src')
            if src:
                src_lower = src.lower()
                if 'super' in src_lower or 'saver' in src_lower:
                    return True
        
        # Strategy 3: Look for aria-label
        saver_aria = await card.query_selector('[aria-label*="super" i], [aria-label*="saver" i]')
        if saver_aria:
            return True
        
        # Strategy 4: Check data attributes
        saver_data = await card.query_selector('[data-badge*="super" i], [data-badge*="saver" i], [data-promotion*="super" i]')
        if saver_data:
            return True
        
        # Strategy 5: Look for badge wrapper classes
        badge_selectors = [
            '[class*="badge"][class*="super" i]',
            '[class*="badge"][class*="saver" i]',
            '[class*="promo"][class*="super" i]',
            '[class*="label"][class*="saver" i]',
        ]
        
        for selector in badge_selectors:
            try:
                elem = await card.query_selector(selector)
                if elem:
                    return True
            except:
                pass
        
        return False
    
    async def extract_sku(self, card) -> str:
        """Extract SKU/product ID"""
        
        for attr in ['data-stockcode', 'data-sku', 'data-product-id', 'data-testid']:
            val = await card.get_attribute(attr)
            if val:
                match = re.search(r'\d+', val)
                if match:
                    return match.group()
        
        return None
    
    async def extract_brand(self, card) -> str:
        """Extract brand"""
        
        brand_elem = await card.query_selector('[class*="brand"], [class*="Brand"]')
        if brand_elem:
            return await brand_elem.inner_text()
        
        return None
    
    async def scrape_all(self) -> List[Dict]:
        """Scrape all pages"""
        
        logger.info("🥩 Starting New World scrape (V4 - Club Deal price fix)")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                ]
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='en-NZ',
                timezone_id='Pacific/Auckland',
            )
            
            await context.set_extra_http_headers({
                'Accept-Language': 'en-NZ,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            })
            
            page = await context.new_page()
            
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)
            
            page_num = 1
            max_pages = 100
            
            while page_num <= max_pages:
                url = f"{self.base_url}?pg={page_num}"
                logger.info(f"📄 Fetching page {page_num}...")
                
                try:
                    if page_num > 1:
                        delay = random.uniform(2, 4)
                        await asyncio.sleep(delay)
                    
                    await page.goto(url, wait_until='domcontentloaded', timeout=45000)
                    await asyncio.sleep(3 if page_num == 1 else 6)
                    
                    # Human-like scrolling
                    for _ in range(3):
                        await page.evaluate(f'window.scrollBy(0, {random.randint(300, 600)})')
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
        
        logger.info(f"✅ Scraped {len(self.products)} total products from {page_num-1} pages")
        return self.products
    
    def save_to_csv(self, filename: str = None):
        """Save products to CSV"""
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'newworld_specials_{timestamp}.csv'
        
        df = pd.DataFrame(self.products)
        
        column_order = ['store', 'sku', 'name', 'brand', 'sale_price', 'original_price', 
                       'price_per_kg', 'unit_type', 'saving', 'is_club_deal', 'is_super_saver', 'scraped_at']
        column_order = [col for col in column_order if col in df.columns]
        df = df[column_order]
        
        df.to_csv(filename, index=False)
        logger.info(f"💾 Saved to {filename}")
        
        # Show stats
        if len(df) > 0:
            logger.info(f"📊 Stats:")
            logger.info(f"   Total products: {len(df)}")
            logger.info(f"   Price range: ${df['sale_price'].min():.2f} - ${df['sale_price'].max():.2f}")
            logger.info(f"   Avg price: ${df['sale_price'].mean():.2f}")
            
            club_deals = df[df['is_club_deal'] == True]
            super_savers = df[df['is_super_saver'] == True]
            
            # Show discount stats
            discounted = df[df['saving'] > 0]
            if len(discounted) > 0:
                logger.info(f"   Products with discounts: {len(discounted)}")
                logger.info(f"   Average discount: ${discounted['saving'].mean():.2f}")
            
            if len(club_deals) > 0:
                logger.info(f"   Club Deals: {len(club_deals)} products")
            if len(super_savers) > 0:
                logger.info(f"   Super Savers: {len(super_savers)} products")
        
        return filename


def main():
    parser = argparse.ArgumentParser(description='New World Scraper - FIXED V4')
    parser.add_argument('--run-once', action='store_true', help='Run scraper once and exit')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    scraper = NewWorldScraper(headless=args.headless)
    
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


if __name__ == '__main__':
    main()