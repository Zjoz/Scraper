"""Recreate the info tables for a range of scrapes (version 1.0).

Since the scraper (scrape_site.py) can add extracted as well as derived
information to a scrape database, this module is basically for maintenance.
Basic use case is when contents or structure of the extracted_info or
derived_info tables is changed since a specific release. By using this module
these tables can then be recreated in earlier scrape databases.
"""

import logging
import re
from pathlib import Path

from scraper_lib import ScrapeDB, setup_file_logging
from scraper_lib import add_extracted_info, add_derived_info

# ============================================================================ #
min_timestamp = '200928-0000'   # scrapes before are not processed
max_timestamp = '201006-2359'   # scrapes after are not processed
extract_info = False            # creates new extracted_info table
derive_info = True              # creates new derived_info table
within_bd = False               # True when running on the DWB
# ============================================================================ #

# establish master scrape directory
if within_bd:
    master_dir = Path('C:/Users', 'diepj09', 'Documents/scrapes')
else:
    master_dir = Path('/home/jos/bdscraper/scrapes')
dirs = sorted([d for d in master_dir.iterdir() if d.is_dir()])

# cycle over all scrape directories
for scrape_dir in dirs:
    db_file = scrape_dir / 'scrape.db'
    if not db_file.exists():
        # directory does not contain a scrape database; get next dir
        continue
    db = ScrapeDB(db_file, create=False)
    timestamp = db.get_par('timestamp')
    if timestamp <= min_timestamp or timestamp >= max_timestamp:
        # scrape is not within timestamp range; get next one
        db.close()
        continue

    setup_file_logging(str(scrape_dir), log_level=logging.INFO)

    # (re)creates the info tables and views in the database
    if extract_info:
        add_extracted_info(db)
    if derive_info:
        add_derived_info(db)

    db.close()
