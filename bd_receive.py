"""Recreate scrape databases after text-based transmission (version 1.1)."""

from pathlib import Path
from bd_viauu import merge_uufiles, uutobin
from prod_scraper_lib import scrape_dirs

# ============================================================================ #
min_timestamp = '201012-0000'   # scrapes before are not processed
max_timestamp = '201012-2359'   # scrapes after are not processed
# ============================================================================ #

master_dir = Path('/home/jos/bdscraper/scrapes')

# cycle over all scrape directories
for timestamp, scrape_dir in scrape_dirs(master_dir, min_timestamp,
                                         max_timestamp, check=False):
    sdb_file = scrape_dir / 'scrape.db'

    first_part_file = scrape_dir / 'scrape.db-01.txt'
    if not first_part_file.exists():
        # directory does not contain part files; get next dir
        continue

    uu_file = merge_uufiles(first_part_file, delete=False)
    uutobin(uu_file)
