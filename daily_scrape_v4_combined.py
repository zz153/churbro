#!/usr/bin/env python3
"""
ChurBro Daily Scraper V4 - Combined Script with Mad Butcher!
Scrapes all 4 stores, cleans data, creates master CSV, and uploads to GitHub
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import json
import os

def run_script(script_name):
    """Run a Python script and return success status"""
    print(f"\n{'='*70}")
    print(f"▶️  Running: {script_name}")
    print('='*70)
    
    try:
        result = subprocess.run(
            ['python3', script_name],
            capture_output=True,
            text=True,
            check=True
        )
        print(result.stdout)
        if result.stderr:
            print("Warnings:", result.stderr)
        print(f"\n✅ {script_name} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running {script_name}:")
        print(e.stderr)
        return False

def combine_csvs():
    """Combine all cleaned CSVs into one master file"""
    print(f"\n{'='*70}")
    print("🔗 COMBINING ALL 4 STORES INTO ONE CSV")
    print('='*70)
    
    # Find the latest cleaned files
    files = {
        'newworld': sorted(Path('.').glob('newworld_cleaned_*.csv'))[-1],
        'paknsave': sorted(Path('.').glob('paknsave_cleaned_*.csv'))[-1],
        'woolworths': sorted(Path('.').glob('woolworths_cleaned_*.csv'))[-1],
        'madbutcher': sorted(Path('.').glob('madbutcher_cleaned_*.csv'))[-1]
    }
    
    dfs = []
    
    # Load New World
    print(f"\n📂 Loading New World data...")
    nw = pd.read_csv(files['newworld'])
    nw_clean = pd.DataFrame({
        'store': 'New World',
        'name': nw['name'],
        'brand': nw['brand'],
        'price': nw['sale_price'],
        'original_price': nw['original_price'],
        'price_per_kg': nw['price_per_kg'],
        'unit_type': nw['unit_type'],
        'saving': nw['saving'],
        'percent_off': nw['percent_off'],
        'deal_type': nw.apply(lambda x: 'Club Deal' if x.get('is_club_deal') else ('Super Saver' if x.get('is_super_saver') else None), axis=1),
        'scraped_at': nw['scraped_at']
    })
    dfs.append(nw_clean)
    print(f"   ✓ New World: {len(nw_clean)} products")
    
    # Load PAK'nSAVE
    print(f"📂 Loading PAK'nSAVE data...")
    ps = pd.read_csv(files['paknsave'])
    ps_clean = pd.DataFrame({
        'store': "PAK'nSAVE",
        'name': ps['name'],
        'brand': ps['brand'],
        'price': ps['price'],
        'original_price': ps.get('promo_price', ps['price']),
        'price_per_kg': ps['price_per_kg'],
        'unit_type': ps['unit_type'],
        'saving': ps.get('saving', 0),
        'percent_off': ps.get('percent_off', 0),
        'deal_type': ps.get('badge_type'),
        'scraped_at': ps['scraped_at']
    })
    dfs.append(ps_clean)
    print(f"   ✓ PAK'nSAVE: {len(ps_clean)} products")
    
    # Load Woolworths
    print(f"📂 Loading Woolworths data...")
    ww = pd.read_csv(files['woolworths'])
    ww_clean = pd.DataFrame({
        'store': 'Woolworths',
        'name': ww['name'],
        'brand': ww['brand'],
        'price': ww['sale_price'],
        'original_price': ww['original_price'],
        'price_per_kg': None,
        'unit_type': 'ea',
        'saving': ww['saving'],
        'percent_off': ww['percent_off'],
        'deal_type': ww.apply(lambda x: 'Club Price' if x.get('is_club_price') else ('On Special' if x.get('is_on_special') else None), axis=1),
        'scraped_at': ww['scraped_at']
    })
    dfs.append(ww_clean)
    print(f"   ✓ Woolworths: {len(ww_clean)} products")
    
    # Load Mad Butcher
    print(f"📂 Loading Mad Butcher data...")
    mb = pd.read_csv(files['madbutcher'])
    mb_clean = pd.DataFrame({
        'store': 'Mad Butcher',
        'name': mb['name'],
        'brand': mb['brand'],
        'price': mb['price'],
        'original_price': mb['original_price'],
        'price_per_kg': mb['price_per_kg'],
        'unit_type': mb['unit_type'],
        'saving': mb['saving'],
        'percent_off': mb['percent_off'],
        'deal_type': mb.get('badge_type'),
        'scraped_at': mb['scraped_at']
    })
    dfs.append(mb_clean)
    print(f"   ✓ Mad Butcher: {len(mb_clean)} products")
    
    # Combine all
    combined = pd.concat(dfs, ignore_index=True)
    
    # Generate filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'churbro_master_{timestamp}.csv'
    
    # Save
    combined.to_csv(output_file, index=False)
    
    # Stats
    print(f"\n📊 COMBINED STATS:")
    print(f"   Total products: {len(combined)}")
    nw_count = len(combined[combined['store'] == 'New World'])
    ps_count = len(combined[combined['store'] == "PAK'nSAVE"])
    ww_count = len(combined[combined['store'] == 'Woolworths'])
    mb_count = len(combined[combined['store'] == 'Mad Butcher'])
    print(f"   New World:   {nw_count:4d}")
    print(f"   PAK'nSAVE:   {ps_count:4d}")
    print(f"   Woolworths:  {ww_count:4d}")
    print(f"   Mad Butcher: {mb_count:4d}")
    print(f"   Price range: ${combined['price'].min():.2f} - ${combined['price'].max():.2f}")
    print(f"   Products with deals: {combined['deal_type'].notna().sum()}")
    print(f"   Products with discounts: {(combined['saving'] > 0).sum()}")
    
    print(f"\n✅ SAVED MASTER CSV: {output_file}")
    print(f"   Columns: {', '.join(combined.columns)}")
    
    return output_file

def organize_files(master_csv):
    """Move all files into a dated folder"""
    print(f"\n{'='*70}")
    print("📁 PHASE 4: ORGANIZING FILES")
    print('='*70)
    
    # Create folder with today's date
    date_str = datetime.now().strftime('%Y-%m-%d')
    folder = Path(f'churbro_data_{date_str}')
    folder.mkdir(exist_ok=True)
    
    print(f"\nCreating folder: {folder}/")
    
    # Convert master_csv to Path object
    master_csv_path = Path(master_csv)
    
    # Move files
    files_to_move = [
        master_csv_path,
        *Path('.').glob('paknsave_cleaned_*.csv'),
        *Path('.').glob('woolworths_cleaned_*.csv'),
        *Path('.').glob('newworld_cleaned_*.csv'),
        *Path('.').glob('madbutcher_cleaned_*.csv'),
        *Path('.').glob('woolworths_specials_*.csv'),
        *Path('.').glob('newworld_specials_*.csv'),
        *Path('.').glob('paknsave_deals_*.csv'),
        *Path('.').glob('madbutcher_products_*.csv'),
    ]
    
    moved_count = 0
    for file in files_to_move:
        if file.exists() and file.parent == Path('.'):
            target = folder / file.name
            file.rename(target)
            marker = " ⭐ (MASTER FILE)" if file.name == master_csv else ""
            print(f"  ✓ Moved: {file.name}{marker}")
            moved_count += 1
    
    print(f"\n{'='*70}")
    print("✅ COMPLETE!")
    print('='*70)
    print(f"📊 Moved {moved_count} files to: {folder}/")
    
    return folder / master_csv

def create_api_files(master_csv):
    """Create JSON and CSV files for the web app API"""
    print(f"\n{'='*70}")
    print("📦 CREATING API FILES FOR WEB APP")
    print('='*70)
    
    # Load master CSV
    df = pd.read_csv(master_csv)
    
    # Replace NaN with None (becomes null in JSON)
    df = df.replace({np.nan: None})
    
    # Create API directory
    api_dir = Path('api')
    api_dir.mkdir(exist_ok=True)
    
    # Prepare data
    api_data = {
        'updated_at': datetime.now().isoformat(),
        'total_products': len(df),
        'stores': {
            store: len(df[df['store'] == store])
            for store in df['store'].unique()
        },
        'products': df.to_dict('records')
    }
    
    # Save as JSON
    json_file = api_dir / 'latest.json'
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(api_data, f, indent=2)
    
    # Save as CSV (same as master)
    csv_file = api_dir / 'latest.csv'
    df.to_csv(csv_file, index=False)
    
    # Create metadata file
    metadata = {
        'last_updated': datetime.now().isoformat(),
        'total_products': len(df),
        'stores': list(df['store'].unique()),
        'price_range': {
            'min': float(df['price'].min()),
            'max': float(df['price'].max())
        }
    }
    
    metadata_file = api_dir / 'metadata.json'
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\n✅ Created: {json_file}")
    print(f"   📊 {len(df)} products from 4 stores")
    print(f"   📏 File size: {json_file.stat().st_size / 1024:.1f} KB")
    print(f"✅ Created: {csv_file}")
    print(f"✅ Created: {metadata_file}")
    print(f"\n🌐 Web app will use: {json_file}")

def auto_upload_to_github():
    """Automatically commit and push API files to GitHub"""
    print(f"\n{'='*70}")
    print("🚀 UPLOADING TO GITHUB")
    print('='*70)
    
    # Check if git repo exists
    if not Path('.git').exists():
        print("\n⚠️  Not a git repository yet")
        print("💡 To enable auto-upload:")
        print("   1. Run: git init")
        print("   2. Run: git remote add origin https://github.com/YOUR-USERNAME/churbro.git")
        print("   3. Follow DEPLOYMENT_GUIDE_V4.md")
        return
    
    try:
        # Add API files
        subprocess.run(['git', 'add', 'api/'], check=True)
        
        # Commit with timestamp
        commit_msg = f"Update prices - {datetime.now().strftime('%Y-%m-%d %H:%M')} - 4 stores"
        subprocess.run(['git', 'commit', '-m', commit_msg], check=True)
        
        # Push to GitHub
        subprocess.run(['git', 'push', 'origin', 'main'], check=True)
        
        print("\n✅ Pushed to GitHub successfully!")
        print("🌐 GitHub Pages will update in 1-2 minutes")
        
    except subprocess.CalledProcessError as e:
        print(f"\n⚠️  Git operation failed: {e}")
        print("💡 You may need to:")
        print("   1. Configure git credentials")
        print("   2. Set up GitHub remote")
        print("   3. Check DEPLOYMENT_GUIDE_V4.md for setup instructions")

def main():
    """Main execution flow"""
    print("\n" + "="*70)
    print("🥩 CHURBRO DAILY SCRAPER V4 - WITH MAD BUTCHER!")
    print("="*70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # PHASE 1: Scraping
    print("\n🕷️  PHASE 1: SCRAPING 4 STORES")
    print("="*70)
    
    scrapers = [
        'automated_scraper_NW_FIXED_V3.py',
        'automated_scraper_PS_FIXED_V3.py',
        'automated_scraper_WW_FIXED.py',  # No _V3 suffix!
        'automated_scraper_MB.py'
    ]
    
    for scraper in scrapers:
        if not run_script(scraper):
            print(f"\n❌ Failed at scraping phase: {scraper}")
            sys.exit(1)
    
    # PHASE 2: Cleaning
    print("\n\n🧹 PHASE 2: CLEANING DATA")
    print("="*70)
    
    cleaners = [
        'cleanup_newworld_v3.py',
        'cleanup_paknsave_v3.py',
        'cleanup_woolworths.py',  # No _v3 suffix!
        'cleanup_madbutcher.py'
    ]
    
    for cleaner in cleaners:
        if not run_script(cleaner):
            print(f"\n❌ Failed at cleaning phase: {cleaner}")
            sys.exit(1)
    
    # PHASE 3: Combining
    print("\n\n🔗 PHASE 3: COMBINING DATA")
    print("="*70)
    
    master_csv = combine_csvs()
    
    # PHASE 4: Organizing
    master_csv_path = organize_files(master_csv)
    
    print(f"\n⏰ Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\n🚀 USE THIS FILE IN CHURBRO APP:")
    print(f"   📄 {master_csv_path}")
    print(f"\n   This file has ALL 4 STORES combined!")
    print(f"   Stores: New World, PAK'nSAVE, Woolworths, Mad Butcher")
    print(f"   Columns: store, name, brand, price, original_price, price_per_kg,")
    print(f"            unit_type, saving, percent_off, deal_type, scraped_at\n")
    
    # PHASE 5: Create API files
    print("\n\n📦 PHASE 5: CREATING WEB APP API")
    print("="*70)
    create_api_files(master_csv_path)
    
    # PHASE 6: Upload to GitHub
    print("\n\n🚀 PHASE 6: UPLOADING TO GITHUB")
    print("="*70)
    auto_upload_to_github()
    
    # Final summary
    print(f"\n{'='*70}")
    print("🎉 ALL DONE!")
    print('='*70)
    print("✅ Scraped all 4 stores (NW, PS, WW, MB)")
    print("✅ Cleaned data")
    print("✅ Combined into master CSV")
    print("✅ Created API files (api/latest.json)")
    print("✅ Uploaded to GitHub (if configured)")
    print(f"\n📱 ChurBro.net will auto-update in 1-2 minutes!")
    print(f"🥩 Now featuring Mad Butcher specialty prices!")
    print('='*70 + "\n")

if __name__ == "__main__":
    main()
