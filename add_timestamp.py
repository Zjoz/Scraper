"""Add the timestamp to the parameters table"""

import re
import logging
from pathlib import Path
from scraper_lib import ScrapeDB, setup_file_logging

# ============================================================================ #
min_timestamp = '200101-0000'   # scrapes before are not processed
max_timestamp = '201231-2359'   # scrapes after are not processed
within_bd = False               # True when running on the DWB
# ============================================================================ #

# establish scrape directories
if within_bd:
    master_dir = Path('C:/Users', 'diepj09', 'Documents/scrapes')
else:
    master_dir = Path('/home/jos/bdscraper/scrapes')
dirs = sorted([d for d in master_dir.iterdir() if d.is_dir()])

# cycle over all scrape directories
for scrape_dir in dirs:
    match = re.match(r'^(\d{6}-\d{4}) - bd-scrape$', scrape_dir.name)
    if not match:
        # not a valid name for a scrape directory; get next one
        continue
    else:
        timestamp = match.group(1)
        if timestamp < min_timestamp or timestamp > max_timestamp:
            # scrape is not within timestamp range
            continue

    setup_file_logging(str(scrape_dir), log_level=logging.INFO)

    # connect to the scrape database
    db_file = scrape_dir / 'scrape.db'
    db = ScrapeDB(db_file, create=False)
    db.upd_par('timestamp', timestamp)
    db.close()

    logging.info('Added timestamp to parameters table\n')
