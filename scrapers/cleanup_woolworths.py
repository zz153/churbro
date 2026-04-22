#!/usr/bin/env python3
"""
Woolworths Data Cleanup Script
Removes products with price < $5 and products with no real discounts
"""

import pandas as pd
from datetime import datetime
import sys

def clean_woolworths(input_file: str):
    """
    Clean Woolworths data:
    1. Remove products with sale_price < $5
    2. Add 'saving' and 'percent_off' columns
    3. Keep ALL other products (regardless of discount or badge)
    """
    
    print("\n" + "="*70)
    print("🧹 WOOLWORTHS DATA CLEANUP")
    print("="*70)
    
    # Load data
    print(f"\n📁 Loading: {input_file}")
    df = pd.read_csv(input_file)
    initial_count = len(df)
    
    # Add badge columns if they don't exist (older scraper versions)
    if 'is_club_price' not in df.columns:
        print("   ⚠️  Adding missing 'is_club_price' column")
        df['is_club_price'] = False
    if 'is_on_special' not in df.columns:
        print("   ⚠️  Adding missing 'is_on_special' column")
        df['is_on_special'] = False
    
    print(f"\n📊 BEFORE CLEANUP:")
    print(f"   Total products: {initial_count}")
    print(f"   Price range: ${df['sale_price'].min():.2f} - ${df['sale_price'].max():.2f}")
    print(f"   Club Prices: {df['is_club_price'].sum()}")
    print(f"   On Special: {df['is_on_special'].sum()}")
    
    # ONLY RULE: Remove cheap products (< $5)
    print(f"\n🔍 Removing products with sale_price < $5...")
    df = df[df['sale_price'] >= 5.0]
    final_count = len(df)
    removed = initial_count - final_count
    print(f"   ✂️  Removed: {removed} products")
    
    # Calculate savings and percentages for ALL products
    df['saving'] = df['original_price'] - df['sale_price']
    df['percent_off'] = ((df['original_price'] - df['sale_price']) / df['original_price'] * 100).round(1)
    
    # Summary statistics
    print(f"\n📊 AFTER CLEANUP:")
    print(f"   Total products: {final_count}")
    print(f"   Total removed: {removed} ({((removed) / initial_count * 100):.1f}%)")
    print(f"   Price range: ${df['sale_price'].min():.2f} - ${df['sale_price'].max():.2f}")
    print(f"   Club Prices: {df['is_club_price'].sum()}")
    print(f"   On Special: {df['is_on_special'].sum()}")
    print(f"   Products with discount: {(df['saving'] > 0).sum()}")
    print(f"   Average saving (all): ${df['saving'].mean():.2f} ({df['percent_off'].mean():.1f}% off)")
    if (df['saving'] > 0).sum() > 0:
        print(f"   Average saving (discounted only): ${df[df['saving'] > 0]['saving'].mean():.2f} ({df[df['saving'] > 0]['percent_off'].mean():.1f}% off)")
    
    # Show top 10 products with best discounts
    discounted = df[df['saving'] > 0]
    if len(discounted) > 0:
        print(f"\n🏆 TOP 10 DEALS (by % off):")
        top_deals = discounted.nlargest(10, 'percent_off')[['name', 'sale_price', 'original_price', 'saving', 'percent_off', 'is_club_price', 'is_on_special']]
        for i, row in enumerate(top_deals.itertuples(), 1):
            badge = "CLUB" if row.is_club_price else ("SPECIAL" if row.is_on_special else "NONE")
            print(f"   {i:2d}. {row.name[:40]:40s} ${row.sale_price:6.2f} (was ${row.original_price:.2f}) -{row.percent_off:4.1f}% [{badge}]")
    else:
        print(f"\n⚠️  No products with discounts found")
    
    # Save cleaned data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"woolworths_cleaned_{timestamp}.csv"
    df.to_csv(output_file, index=False)
    
    print(f"\n✅ SAVED: {output_file}")
    print(f"   Columns: {', '.join(df.columns)}\n")
    
    return df, output_file

if __name__ == '__main__':
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        # Auto-detect Woolworths CSV file
        import glob
        files = glob.glob('woolworths_*.csv')
        files = [f for f in files if 'cleaned' not in f]
        
        if not files:
            print("❌ No Woolworths CSV file found!")
            print("Usage: python cleanup_woolworths.py <input_file.csv>")
            sys.exit(1)
        
        input_file = files[0]
    
    try:
        clean_woolworths(input_file)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
