"""Enriches a scrape database with pages information (version 1.0).

All pages from the scrape are complemented with various information that is
extracted from a stored scrape. As such this concerns redundant data, but is
made available to facilitate faster access.
The attributes are made available via the pages_info table or the pages_full
view in the scrape.db.
"""

import os.path
import logging

from scraper_lib import ScrapeDB, setup_file_logging
from scraper_lib import add_pages_info

# TODO: add key_figures table to database, with
#     - number of pages, redirects, links
#     _ number of DV-pages, Bib-pages
#     - number of missing H1's, pages with more than one H1
#     - number of pages with H1/H2 in wrong section (header/footer)
#     - number of pages without title
# TODO: register removed and added pages

# ============================================================================ #
timestamp = '200926-0937'   # determines the scrape that is used
within_bd = False           # True when running on the DWB
# ============================================================================ #

# establish directory of the scrape
if within_bd:
    scrapes_dir = 'C:\\Users\\diepj09\\Documents\\scrapes\\'
else:
    scrapes_dir = '/home/jos/bdscraper/scrapes/'
scrape_dir = scrapes_dir + timestamp + ' - bd-scrape'

# setup context
db_file = os.path.join(scrape_dir, 'scrape.db')
db = ScrapeDB(db_file, create=False)
setup_file_logging(scrape_dir, log_level=logging.INFO)

add_pages_info(db)
