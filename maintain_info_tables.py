"""Maintain the info and links tables for a range of scrapes (version 1.2).

Since all functions of this module are included in the scraper module
(scrape_site.py), this module is for maintenance only.

Basic use case is when contents or structure of the extracted_info or
derived_info tables is changed since a specific release. But there can also
be reasons to repopulate the links tables for a range of scrapes.

By using this module the information tables can be recreated and/or the links
table be repopulated in earlier scrape databases. To recreate the
derived_info tables, the extracted_info tables need to be up to date.
"""

import logging
from pathlib import Path

from scraper_lib import ScrapeDB, setup_file_logging

# ============================================================================ #
min_timestamp = '200901-0000'   # scrapes before are not processed
max_timestamp = '201101-2359'   # scrapes after are not processed
links_table = True              # repopulate links table
renew_info = False              # repopulate pages_info table
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

    setup_file_logging(scrape_dir, log_level=logging.INFO)

    if links_table:
        db.repop_ed_links()

    # update pages_info table
    if renew_info:
        db.extract_pages_info()
        db.derive_pages_info()

    db.close()
