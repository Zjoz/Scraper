"""Prepare scrape databases for text-based transmission (version 1.0)."""

from pathlib import Path

from scraper_lib import ScrapeDB
from bd_viauu import bintouu, split_uufile

# ============================================================================ #
min_timestamp = '201012-0000'   # scrapes before are not processed
max_timestamp = '201012-2359'   # scrapes after are not processed
part_max_mb = 30
# ============================================================================ #

master_dir = Path('/home/jos/bdscraper/scrapes')

# cycle over all scrape directories
dirs = [d for d in master_dir.glob('??????-???? - bd-scrape') if d.is_dir()]
for scrape_dir in dirs:
    timestamp = scrape_dir.name[:11]
    if timestamp <= min_timestamp or timestamp >= max_timestamp:
        # scrape is not within timestamp range; get next one
        continue
    db_file = scrape_dir / 'scrape.db'
    if not db_file.exists():
        # directory does not contain a scrape database; get next dir
        continue

    uu_file = bintouu(db_file)
    split_uufile(uu_file, part_max_mb)
