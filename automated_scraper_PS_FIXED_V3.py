#!/usr/bin/env python3
"""
PAK'nSAVE Scraper - FIXED VERSION V3
- Properly separates unit prices (ea/kg) from per kg reference prices
- Same fix as New World V3 - no more wrong prices!
- Correctly extracts Everyday Low, Extra Low, and Super Deal badges
- Captures: price, price_per_kg, unit_type
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

class PaknsaveScraper:
    def __init__(self, headless=True):
        self.base_url = "https://www.paknsave.co.nz/shop/category/meat-poultry-and-seafood"
        self.headless = headless
        self.products = []
        
    async def scrape_page(self, page, page_num: int) -> List[Dict]:
        """Scrape products from a single page"""
        
        products = []
        
        try:
            # Use correct selector - product cards have testid like "product-5131155-KGM-000"
            selector = '[data-testid^="product-"][data-testid*="-"]'
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
                        
                        # Show first 10 with badge info
                        if i < 10:
                            badges = []
                            if product.get('is_everyday_low'):
                                badges.append('EVERYDAY')
                            if product.get('is_extra_low'):
                                badges.append('EXTRA')
                            if product.get('is_super_deal'):
                                badges.append('SUPER')
                            badge_str = f" [{', '.join(badges)}]" if badges else ""
                            logger.info(f"    ✓ {product.get('name', 'N/A')[:35]:35s} ${product.get('price', 0):.2f}{badge_str}")
                        
                        # Debug badge detection on first product with a badge
                        if i == 0 or (i < 20 and (product.get('is_everyday_low') or product.get('is_extra_low') or product.get('is_super_deal'))):
                            # Check what badge SVG exists
                            badge_div = await card.query_selector('.owfhtzj')
                            if badge_div:
                                badge_svg = await badge_div.query_selector('svg[aria-label]')
                                if badge_svg:
                                    aria = await badge_svg.get_attribute('aria-label')
                                    logger.debug(f"    Badge SVG aria-label: '{aria}'")
                        
                except Exception as e:
                    logger.debug(f"Error parsing card {i}: {e}")
            
            logger.info(f"  Extracted {len(products)} valid products")
                    
        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {e}")
        
        return products
    
    async def parse_product_card(self, card) -> Dict:
        """Extract data from a product card"""
        
        try:
            full_text = await card.inner_text()
            
            # 1. Extract product name
            name = await self.extract_name(card)
            if not name:
                return None
            
            # 2. Extract prices (unit price + per kg reference)
            price_data = await self.extract_all_prices(card, full_text)
            if not price_data:
                return None
            
            # 3. Detect badges
            is_everyday_low = await self.is_everyday_low(card, full_text)
            is_extra_low = await self.is_extra_low(card, full_text)
            is_super_deal = await self.is_super_deal(card, full_text)
            
            if is_everyday_low or is_extra_low or is_super_deal:
                logger.debug(f"    Badge detected: '{name[:30]}' - everyday={is_everyday_low}, extra={is_extra_low}, super={is_super_deal}")
            
            # 4. Extract product ID
            product_id = await self.extract_product_id(card)
            
            # 5. Extract brand
            brand = await self.extract_brand(card)
            
            # PAK'nSAVE doesn't show separate sale/original prices
            # The badge just indicates it's a good deal, but price is the price
            return {
                'store': 'paknsave',
                'product_id': product_id,
                'name': name,
                'brand': brand,
                'price': price_data['price'],
                'price_per_kg': price_data.get('price_per_kg'),
                'unit_type': price_data['unit_type'],
                'promo_price': price_data['price'],  # Keep for backward compatibility
                'saving': 0.0,  # PAK'nSAVE doesn't show savings
                'is_everyday_low': is_everyday_low,
                'is_extra_low': is_extra_low,
                'is_super_deal': is_super_deal,
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.debug(f"Error parsing card: {e}")
            return None
    
    async def extract_name(self, card) -> str:
        """Extract product name"""
        
        # Strategy 1: Look for anchor tags
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
        name_elem = await card.query_selector('[class*="name"], [class*="Name"], [class*="title"]')
        if name_elem:
            return await name_elem.inner_text()
        
        return None
    
    async def extract_all_prices(self, card, full_text: str) -> Dict:
        """
        Extract unit price AND per kg reference price
        
        PAK'nSAVE structure (from screenshot):
        - Unit price: "3.79 ea" or "9.99 kg" (the MAIN price)
        - Per kg reference: "$18.95/1kg" or "$9.99/1kg"
        
        Returns:
        - price: The main unit price (ea or kg)
        - price_per_kg: The per kg reference (for comparison)
        - unit_type: 'ea' or 'kg'
        """
        
        # Step 1: Find UNIT prices (ea or kg) - these are the MAIN prices
        # Pattern: "3.79 ea", "11.49 ea", "9.99 kg", "25.49 kg"
        unit_prices = re.findall(r'(\d+)[.,\s]*(\d{2})\s*(ea|kg|each)', full_text, re.IGNORECASE)
        
        # Step 2: Find PER KG REFERENCE prices
        # Pattern: "$18.95/1kg", "$10.45/kg"
        per_kg_ref = re.findall(r'\$?(\d+\.\d{2})\s*/\s*(?:1)?kg', full_text, re.IGNORECASE)
        
        logger.debug(f"    Price extraction: unit={unit_prices}, per_kg_ref={per_kg_ref}")
        
        if not unit_prices:
            # Fallback: try to find any reasonable price
            all_prices = re.findall(r'(\d+)\.(\d{2})', full_text)
            if all_prices:
                dollars, cents = all_prices[0]
                price = float(f"{dollars}.{cents}")
                if 0.5 < price < 500:
                    return {
                        'price': price,
                        'price_per_kg': None,
                        'unit_type': 'ea'
                    }
            return None
        
        # Extract the first unit price (the displayed price)
        dollars, cents, unit = unit_prices[0]
        main_price = float(f"{dollars}.{cents}")
        unit_type = 'kg' if 'kg' in unit.lower() else 'ea'
        
        # Extract per kg reference
        price_per_kg = None
        if per_kg_ref:
            price_per_kg = float(per_kg_ref[0])
        
        # Validate price
        if not (0.5 < main_price < 500):
            return None
        
        return {
            'price': main_price,
            'price_per_kg': price_per_kg,
            'unit_type': unit_type
        }
    
    async def is_everyday_low(self, card, full_text: str) -> bool:
        """Check for Everyday Low badge (Badge 4701)"""
        
        # PRIMARY: Check for badge 4701 SVG in .owfhtzj div
        badge_svg = await card.query_selector('.owfhtzj svg[aria-label*="4701"]')
        if badge_svg:
            return True
        
        # FALLBACK: Check text (less reliable)
        text_lower = full_text.lower()
        if 'everyday low' in text_lower or 'everydaylow' in text_lower:
            return True
        
        return False
    
    async def is_extra_low(self, card, full_text: str) -> bool:
        """Check for Extra Low badge (Badge 6000)"""
        
        # PRIMARY: Check for badge 6000 SVG in .owfhtzj div
        badge_svg = await card.query_selector('.owfhtzj svg[aria-label*="6000"]')
        if badge_svg:
            return True
        
        # FALLBACK: Check text (less reliable)
        text_lower = full_text.lower()
        if 'extra low' in text_lower or 'extralow' in text_lower:
            return True
        
        return False
    
    async def is_super_deal(self, card, full_text: str) -> bool:
        """Check for Super Deal badge (Badge number unknown - might be 5000, 7000, etc.)"""
        
        # Check for any badge that's NOT 4701 (Everyday) or 6000 (Extra Low)
        # Super Deal might be badge 5000, 7000, or similar
        badge_svg = await card.query_selector('.owfhtzj svg[aria-label]')
        if badge_svg:
            aria = await badge_svg.get_attribute('aria-label') or ''
            # If it has a badge but it's not 4701 or 6000, might be Super Deal
            if 'badge' in aria.lower() and '4701' not in aria and '6000' not in aria:
                return True
        
        # FALLBACK: Check text
        text_lower = full_text.lower()
        if 'super deal' in text_lower or 'superdeal' in text_lower:
            return True
        
        if 'super' in text_lower and 'deal' in text_lower:
            return True
        
        return False
    
    async def extract_product_id(self, card) -> str:
        """Extract product ID"""
        
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
        
        logger.info("🥩 Starting PAK'nSAVE scrape")
        
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
            filename = f'paknsave_deals_{timestamp}.csv'
        
        df = pd.DataFrame(self.products)
        
        column_order = ['store', 'product_id', 'name', 'brand', 'price', 'price_per_kg', 
                       'unit_type', 'promo_price', 'saving', 'is_everyday_low', 
                       'is_extra_low', 'is_super_deal', 'scraped_at']
        column_order = [col for col in column_order if col in df.columns]
        df = df[column_order]
        
        df.to_csv(filename, index=False)
        logger.info(f"💾 Saved to {filename}")
        
        # Show stats
        if len(df) > 0:
            logger.info(f"📊 Stats:")
            logger.info(f"   Total products: {len(df)}")
            logger.info(f"   Price range: ${df['price'].min():.2f} - ${df['price'].max():.2f}")
            logger.info(f"   Avg price: ${df['price'].mean():.2f}")
            
            everyday = df[df['is_everyday_low'] == True]
            extra = df[df['is_extra_low'] == True]
            super_deals = df[df['is_super_deal'] == True]
            
            if len(everyday) > 0:
                logger.info(f"   Everyday Low: {len(everyday)} products")
            if len(extra) > 0:
                logger.info(f"   Extra Low: {len(extra)} products")
            if len(super_deals) > 0:
                logger.info(f"   Super Deals: {len(super_deals)} products")
        
        return filename


def main():
    parser = argparse.ArgumentParser(description='PAK\'nSAVE Scraper')
    parser.add_argument('--run-once', action='store_true', help='Run scraper once and exit')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    scraper = PaknsaveScraper(headless=args.headless)
    
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
