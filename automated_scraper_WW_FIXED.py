#!/usr/bin/env python3
"""
Woolworths Scraper - FIXED VERSION V2
Extracts the CORRECT pack price and FILTERS OUT price text from product names
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

class WoolworthsScraper:
    def __init__(self, headless=True):
        self.base_url = "https://www.woolworths.co.nz/shop/browse/meat-poultry"
        self.headless = headless
        self.products = []
        
    async def scrape_page(self, page, page_num: int) -> List[Dict]:
        """Scrape products from a single page"""
        
        products = []
        
        try:
            selector = '.product-entry'
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
                            logger.info(f"    ✓ {product.get('name', 'N/A')[:40]:40s} ${product.get('sale_price', 0):.2f}")
                except Exception as e:
                    logger.debug(f"Error parsing card {i}: {e}")
            
            logger.info(f"  Extracted {len(products)} valid products")
                    
        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {e}")
        
        return products
    
    async def parse_product_card(self, card) -> Dict:
        """Extract data from a product card - FIXED VERSION"""
        
        try:
            # 1. GET PRODUCT NAME
            name = await self.extract_name(card)
            if not name:
                return None
            
            # 2. GET THE ACTUAL PACK PRICE (the big bold price)
            sale_price = await self.extract_pack_price(card)
            if not sale_price:
                return None
            
            # 3. GET ORIGINAL/WAS PRICE (if exists)
            original_price = await self.extract_original_price(card)
            if not original_price:
                original_price = sale_price  # No discount
            
            # 4. GET SKU
            sku = await self.extract_sku(card)
            
            # 5. GET BRAND
            brand = await self.extract_brand(card, name)
            
            # Calculate saving
            saving = 0
            if original_price and sale_price and original_price > sale_price:
                saving = round(original_price - sale_price, 2)
            
            return {
                'store': 'woolworths',
                'sku': sku,
                'name': name,
                'brand': brand,
                'sale_price': sale_price,
                'original_price': original_price,
                'saving': saving,
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.debug(f"Error parsing card: {e}")
            return None
    
    async def extract_name(self, card) -> str:
        """Extract product name - IMPROVED with price filtering"""
        
        def is_valid_product_name(text: str) -> bool:
            """Check if text is a valid product name, not a price"""
            if not text or len(text.strip()) < 10:
                return False
            
            text = text.strip()
            
            # Reject if it's just price-like text
            if text.startswith('$'):
                return False
            
            # Reject if it's mostly numbers (like "5 20" or "8 80")
            numbers_only = re.sub(r'[^\d]', '', text)
            if len(numbers_only) > 0 and len(numbers_only) / len(text) > 0.5:  # More than 50% numbers
                return False
            
            # Reject if it contains price patterns
            if re.match(r'^\$?\d+[\s\n.]\d+$', text):
                return False
            
            # Must contain at least some letters
            if not re.search(r'[a-zA-Z]{3,}', text):  # At least 3 letters in a row
                return False
            
            return True
        
        # Strategy 1: Look for heading tags (most reliable)
        for tag in ['h3', 'h2', 'h1', 'h4']:
            elem = await card.query_selector(tag)
            if elem:
                text = await elem.inner_text()
                if is_valid_product_name(text):
                    return text.strip()
        
        # Strategy 2: Look for product title link
        link = await card.query_selector('a[class*="product"]')
        if link:
            text = await link.inner_text()
            lines = text.split('\n')
            for line in lines:
                if is_valid_product_name(line):
                    return line.strip()
        
        # Strategy 3: Try aria-label (often has clean product name)
        aria_label = await card.get_attribute('aria-label')
        if aria_label and is_valid_product_name(aria_label):
            return aria_label.strip()
        
        # Strategy 4: Title attribute
        title = await card.get_attribute('title')
        if title and is_valid_product_name(title):
            return title.strip()
        
        return None
    
    async def extract_pack_price(self, card) -> float:
        """
        Extract the ACTUAL PACK PRICE - the big bold number you pay for ONE pack
        NOT multi-buy deals, NOT unit prices
        """
        
        # Strategy 1: Look for price-dollars and price-cents (Woolworths format)
        dollars_elem = await card.query_selector('.price-dollars, [class*="price-dollar"]')
        cents_elem = await card.query_selector('.price-cents, [class*="price-cent"]')
        
        if dollars_elem and cents_elem:
            try:
                dollars_text = await dollars_elem.inner_text()
                cents_text = await cents_elem.inner_text()
                
                # Extract just the numbers
                dollars = re.sub(r'[^\d]', '', dollars_text)
                cents = re.sub(r'[^\d]', '', cents_text)
                
                if dollars:
                    price = float(f"{dollars}.{cents if cents else '00'}")
                    if 1 < price < 500:  # Sanity check
                        return price
            except:
                pass
        
        # Strategy 2: Look for the primary price element (largest/boldest)
        # This is typically in a class like "price-dollars" or "product-price"
        price_selectors = [
            '[class*="price-dollar"]',
            '[class*="product-price"]',
            '[class*="current-price"]',
            '.price',
        ]
        
        for selector in price_selectors:
            elem = await card.query_selector(selector)
            if elem:
                text = await elem.inner_text()
                # Extract price from text like "$8.50" or "$8 50"
                match = re.search(r'\$?(\d+)[\s.]?(\d{2})?', text)
                if match:
                    dollars = match.group(1)
                    cents = match.group(2) if match.group(2) else "00"
                    price = float(f"{dollars}.{cents}")
                    
                    # Filter out multi-buy prices (look for "for $" pattern nearby)
                    parent_text = await card.inner_text()
                    if f"for ${dollars}" not in parent_text:  # Not a multi-buy
                        if 1 < price < 500:
                            return price
        
        # Strategy 3: Get all prices and filter intelligently
        full_text = await card.inner_text()
        
        # Find all prices
        price_matches = re.findall(r'\$(\d+)\.(\d{2})', full_text)
        
        if price_matches:
            prices = []
            for match in price_matches:
                price = float(f"{match[0]}.{match[1]}")
                
                # Skip if this is clearly a multi-buy deal
                # Check if "X for $price" appears before this price
                price_str = f"${match[0]}.{match[1]}"
                if f"for {price_str}" in full_text:
                    continue  # Skip multi-buy prices
                
                # Skip unit prices (too high for a pack)
                if price > 100:
                    continue
                
                # Skip unrealistic prices
                if price < 1 or price > 500:
                    continue
                
                prices.append(price)
            
            if prices:
                # The actual pack price is usually the SMALLEST valid price
                # (multi-buy deals are larger, unit prices are larger)
                return min(prices)
        
        return None
    
    async def extract_original_price(self, card) -> float:
        """Extract 'was' price if product is on sale"""
        
        full_text = await card.inner_text()
        
        # Look for "was $XX.XX" pattern
        was_match = re.search(r'was\s+\$(\d+)\.(\d{2})', full_text, re.IGNORECASE)
        if was_match:
            return float(f"{was_match.group(1)}.{was_match.group(2)}")
        
        # Look for crossed-out price elements
        strikethrough_selectors = [
            '[class*="was"]',
            '[class*="crossed"]',
            '[class*="original"]',
            'del',
            's',
        ]
        
        for selector in strikethrough_selectors:
            elem = await card.query_selector(selector)
            if elem:
                text = await elem.inner_text()
                match = re.search(r'\$?(\d+)\.(\d{2})', text)
                if match:
                    return float(f"{match.group(1)}.{match.group(2)}")
        
        return None
    
    async def extract_sku(self, card) -> str:
        """Extract SKU/product ID"""
        
        for attr in ['data-stockcode', 'data-sku', 'data-product-id', 'stockcode']:
            val = await card.get_attribute(attr)
            if val:
                match = re.search(r'\d+', val)
                if match:
                    return match.group()
        
        # Try to extract from URL
        href = await card.get_attribute('href')
        if not href:
            link = await card.query_selector('a')
            if link:
                href = await link.get_attribute('href')
        
        if href:
            match = re.search(r'/product/(\d+)', href)
            if match:
                return match.group(1)
        
        return None
    
    async def extract_brand(self, card, name: str) -> str:
        """Extract brand"""
        
        brand_elem = await card.query_selector('[class*="brand"], [class*="Brand"]')
        if brand_elem:
            return await brand_elem.inner_text()
        
        if name and 'woolworths' in name.lower():
            return 'Woolworths'
        
        return None
    
    async def scrape_all(self) -> List[Dict]:
        """Scrape all pages"""
        
        logger.info("🥩 Starting Woolworths scrape (FIXED VERSION V2)")
        
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
                # FORCE DUNEDIN LOCATION!
                geolocation={'latitude': -45.8788, 'longitude': 170.5028},  # Dunedin CBD
                permissions=['geolocation'],
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
                url = f"{self.base_url}?store=dunedin&page={page_num}&inStockProductsOnly=false"
                logger.info(f"📄 Fetching page {page_num} from DUNEDIN...")
                
                try:
                    if page_num > 1:
                        delay = random.uniform(2, 4)
                        await asyncio.sleep(delay)
                    
                    await page.goto(url, wait_until='domcontentloaded', timeout=45000)
                    await asyncio.sleep(4 if page_num == 1 else 6)
                    
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
            filename = f'woolworths_specials_{timestamp}.csv'
        
        df = pd.DataFrame(self.products)
        
        column_order = ['store', 'sku', 'name', 'brand', 'sale_price', 'original_price', 'saving', 'scraped_at']
        column_order = [col for col in column_order if col in df.columns]
        df = df[column_order]
        
        df.to_csv(filename, index=False)
        logger.info(f"💾 Saved to {filename}")
        
        # Show stats
        if len(df) > 0:
            logger.info(f"📊 Stats:")
            logger.info(f"   Products: {len(df)}")
            logger.info(f"   Price range: ${df['sale_price'].min():.2f} - ${df['sale_price'].max():.2f}")
            logger.info(f"   Avg price: ${df['sale_price'].mean():.2f}")
            on_sale = df[df['saving'] > 0]
            if len(on_sale) > 0:
                logger.info(f"   On sale: {len(on_sale)} products")
                logger.info(f"   Avg saving: ${on_sale['saving'].mean():.2f}")
        
        return filename


def main():
    parser = argparse.ArgumentParser(description='Woolworths Scraper - FIXED V2')
    parser.add_argument('--run-once', action='store_true', help='Run scraper once and exit')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    scraper = WoolworthsScraper(headless=args.headless)
    
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
