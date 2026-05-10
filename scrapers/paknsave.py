#!/usr/bin/env python3
"""
PAK'nSAVE Scraper - FIXED VERSION V4
- Scrapes fresh meat/seafood + frozen meat + frozen fish & seafood
- Adds 'category' field: 'fresh' or 'frozen'
- Properly separates unit prices (ea/kg) from per kg reference prices
- Correctly extracts Everyday Low, Extra Low, and Super Deal badges
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
        self.categories = [
            # Fresh meat, poultry & seafood (existing)
            {
                'url': 'https://www.paknsave.co.nz/shop/category/meat-poultry-and-seafood',
                'label': 'fresh'
            },
            # Frozen chicken & meat subcategories
            {
                'url': 'https://www.paknsave.co.nz/shop/category/frozen/frozen-chicken--meat/frozen-coated-chicken--nuggets',
                'label': 'frozen'
            },
            {
                'url': 'https://www.paknsave.co.nz/shop/category/frozen/frozen-chicken--meat/frozen-whole-chicken--portions',
                'label': 'frozen'
            },
            {
                'url': 'https://www.paknsave.co.nz/shop/category/frozen/frozen-chicken--meat/frozen-beef--lamb--pork',
                'label': 'frozen'
            },
            {
                'url': 'https://www.paknsave.co.nz/shop/category/frozen/frozen-chicken--meat/frozen-burger-patties',
                'label': 'frozen'
            },
            {
                'url': 'https://www.paknsave.co.nz/shop/category/frozen/frozen-chicken--meat/frozen-meat-alternatives',
                'label': 'frozen'
            },
            # Frozen fish & seafood subcategories
            {
                'url': 'https://www.paknsave.co.nz/shop/category/frozen/frozen-fish--seafood/frozen-fish',
                'label': 'frozen'
            },
            {
                'url': 'https://www.paknsave.co.nz/shop/category/frozen/frozen-fish--seafood/frozen-prawns--other-seafood',
                'label': 'frozen'
            },
        ]
        self.headless = headless
        self.products = []
        
    async def scrape_page(self, page, page_num: int) -> List[Dict]:
        """Scrape products from a single page"""
        
        products = []
        
        try:
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
                        
                        if i == 0 or (i < 20 and (product.get('is_everyday_low') or product.get('is_extra_low') or product.get('is_super_deal'))):
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
            
            name = await self.extract_name(card)
            if not name:
                return None
            
            price_data = await self.extract_all_prices(card, full_text)
            if not price_data:
                return None
            
            is_everyday_low = await self.is_everyday_low(card, full_text)
            is_extra_low = await self.is_extra_low(card, full_text)
            is_super_deal = await self.is_super_deal(card, full_text)
            
            if is_everyday_low or is_extra_low or is_super_deal:
                logger.debug(f"    Badge detected: '{name[:30]}' - everyday={is_everyday_low}, extra={is_extra_low}, super={is_super_deal}")
            
            product_id = await self.extract_product_id(card)
            brand = await self.extract_brand(card)
            
            return {
                'store': 'paknsave',
                'product_id': product_id,
                'name': name,
                'brand': brand,
                'category': None,  # will be set in scrape_all per category
                'price': price_data['price'],
                'price_per_kg': price_data.get('price_per_kg'),
                'unit_type': price_data['unit_type'],
                'promo_price': price_data['price'],
                'saving': 0.0,
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
        
        anchors = await card.query_selector_all('a')
        for anchor in anchors:
            text = await anchor.inner_text()
            text = text.strip()
            
            if text and len(text) > 5 and not text.startswith('$'):
                skip_words = ['add', 'view', 'cart', 'more', 'details', 'shop', 'buy']
                if not any(word in text.lower() for word in skip_words):
                    return text
        
        for tag in ['h3', 'h2', 'h1', 'h4']:
            elem = await card.query_selector(tag)
            if elem:
                text = await elem.inner_text()
                if text and len(text.strip()) > 5:
                    return text.strip()
        
        name_elem = await card.query_selector('[class*="name"], [class*="Name"], [class*="title"]')
        if name_elem:
            return await name_elem.inner_text()
        
        return None
    
    async def extract_all_prices(self, card, full_text: str) -> Dict:
        """Extract unit price AND per kg reference price"""
        
        unit_prices = re.findall(r'(\d+)[.,\s]*(\d{2})\s*(ea|kg|each)', full_text, re.IGNORECASE)
        per_kg_ref = re.findall(r'\$?(\d+\.\d{2})\s*/\s*(?:1)?kg', full_text, re.IGNORECASE)
        
        logger.debug(f"    Price extraction: unit={unit_prices}, per_kg_ref={per_kg_ref}")
        
        if not unit_prices:
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
        
        dollars, cents, unit = unit_prices[0]
        main_price = float(f"{dollars}.{cents}")
        unit_type = 'kg' if 'kg' in unit.lower() else 'ea'
        
        price_per_kg = None
        if per_kg_ref:
            price_per_kg = float(per_kg_ref[0])
        
        if not (0.5 < main_price < 500):
            return None
        
        return {
            'price': main_price,
            'price_per_kg': price_per_kg,
            'unit_type': unit_type
        }
    
    async def is_everyday_low(self, card, full_text: str) -> bool:
        badge_svg = await card.query_selector('.owfhtzj svg[aria-label*="4701"]')
        if badge_svg:
            return True
        text_lower = full_text.lower()
        if 'everyday low' in text_lower or 'everydaylow' in text_lower:
            return True
        return False
    
    async def is_extra_low(self, card, full_text: str) -> bool:
        badge_svg = await card.query_selector('.owfhtzj svg[aria-label*="6000"]')
        if badge_svg:
            return True
        text_lower = full_text.lower()
        if 'extra low' in text_lower or 'extralow' in text_lower:
            return True
        return False
    
    async def is_super_deal(self, card, full_text: str) -> bool:
        badge_svg = await card.query_selector('.owfhtzj svg[aria-label]')
        if badge_svg:
            aria = await badge_svg.get_attribute('aria-label') or ''
            if 'badge' in aria.lower() and '4701' not in aria and '6000' not in aria:
                return True
        text_lower = full_text.lower()
        if 'super deal' in text_lower or 'superdeal' in text_lower:
            return True
        if 'super' in text_lower and 'deal' in text_lower:
            return True
        return False
    
    async def extract_product_id(self, card) -> str:
        for attr in ['data-stockcode', 'data-sku', 'data-product-id', 'data-testid']:
            val = await card.get_attribute(attr)
            if val:
                match = re.search(r'\d+', val)
                if match:
                    return match.group()
        return None
    
    async def extract_brand(self, card) -> str:
        brand_elem = await card.query_selector('[class*="brand"], [class*="Brand"]')
        if brand_elem:
            return await brand_elem.inner_text()
        return None
    
    async def scrape_all(self) -> List[Dict]:
        """Scrape all categories"""
        
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
                geolocation={'latitude': -45.8788, 'longitude': 170.5028},
                permissions=['geolocation'],
            )
            
            await context.set_extra_http_headers({
                'Accept-Language': 'en-NZ,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            })

            page = await context.new_page()

            await page.add_init_script("""
                localStorage.setItem('selectedStore', 'DUNEDIN');
                localStorage.setItem('store', 'dunedin');
                document.cookie = "store=dunedin; path=/; SameSite=Lax";
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            for category in self.categories:
                base_url = category['url']
                label = category['label']
                cat_name = base_url.split('/')[-1]
                logger.info(f"\n📦 Scraping: {cat_name} [{label}]")

                page_num = 1
                max_pages = 100

                while page_num <= max_pages:
                    url = f"{base_url}?store=dunedin&pg={page_num}"
                    logger.info(f"📄 Fetching page {page_num}...")
                    
                    try:
                        if page_num > 1:
                            delay = random.uniform(2, 4)
                            await asyncio.sleep(delay)
                        
                        await page.goto(url, wait_until='domcontentloaded', timeout=45000)
                        await asyncio.sleep(3 if page_num == 1 else 6)
                        
                        for _ in range(3):
                            await page.evaluate(f'window.scrollBy(0, {random.randint(300, 600)})')
                            await asyncio.sleep(random.uniform(0.5, 1.5))
                        
                        await page.mouse.move(random.randint(100, 800), random.randint(100, 600))
                        await asyncio.sleep(0.5)
                        
                        page_products = await self.scrape_page(page, page_num)
                        
                        if not page_products:
                            logger.info(f"  No products on page {page_num}, moving to next category")
                            break
                        
                        # Tag each product with fresh/frozen label
                        for product in page_products:
                            product['category'] = label
                        
                        self.products.extend(page_products)
                        page_num += 1
                        
                    except Exception as e:
                        logger.error(f"Error on page {page_num}: {e}")
                        break
            
            await browser.close()
        
        logger.info(f"\n✅ Scraped {len(self.products)} total products")

        # Summary by category
        fresh = [p for p in self.products if p.get('category') == 'fresh']
        frozen = [p for p in self.products if p.get('category') == 'frozen']
        logger.info(f"   Fresh: {len(fresh)} products")
        logger.info(f"   Frozen: {len(frozen)} products")

        return self.products
    
    def save_to_csv(self, filename: str = None):
        """Save products to CSV"""
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'paknsave_deals_{timestamp}.csv'
        
        df = pd.DataFrame(self.products)
        
        column_order = ['store', 'product_id', 'name', 'brand', 'category', 'price', 'price_per_kg', 
                       'unit_type', 'promo_price', 'saving', 'is_everyday_low', 
                       'is_extra_low', 'is_super_deal', 'scraped_at']
        column_order = [col for col in column_order if col in df.columns]
        df = df[column_order]
        
        df.to_csv(filename, index=False)
        logger.info(f"💾 Saved to {filename}")
        
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
