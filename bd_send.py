"""Prepare scrape databases for text-based transmission (version 1.2)."""

from pathlib import Path
from bd_viauu import bintouu, split_uufile
from prod_scraper_lib import scrape_dirs

# TODO: add master database

# ============================================================================ #
min_timestamp = '200831-0000'   # scrapes before are not processed
max_timestamp = '201231-2359'   # scrapes after are not processed
part_max_mb = 30
# ============================================================================ #

master_dir = Path('/home/jos/bdscraper/scrapes')

# cycle over all scrape directories
for timestamp, scrape_dir in scrape_dirs(master_dir, min_timestamp,
                                         max_timestamp):
    sdb_file = scrape_dir / 'scrape.db'

    uu_file = bintouu(sdb_file)
    split_uufile(uu_file, part_max_mb)
