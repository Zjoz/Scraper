"""Enriches a scrape database with various page attributes (version 1.0).

All pages from the scrape are complemented with several attributes that are
extracted from the scrape that was stored already. As such this concerns
redundant data, but is made available to facilitate faster access.
The attributes are made available via the attribs table or the pages_attribs
view in the scrape.db.
"""

import time
import os.path
import logging
from bs4 import BeautifulSoup

from scraper_lib import ScrapeDB, setup_file_logging
from scraper_lib import title, language, mod_date, page_type, h1s, classes

# TODO: add key_figures table to database
# TODO: register removed and added pages

# ============================================================================ #
timestamp = '200924-1711'   # determines the scrape that is used
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
num_pages = db.num_pages()
start_time = time.time()

logging.info('Database enrichment started')
db.new_attribs()

page_num = 0
for pi, pp, ps in db.pages():
    page_num += 1
    soup = BeautifulSoup(ps, features='lxml')

    h1l = h1s(soup, pp)
    if h1l:
        nh1 = len(h1l)
        h1 = h1l[0]
    else:
        nh1 = 0
        h1 = None
    if cl := classes(soup, pp):
        cl = ' '.join(map(str, cl))

    db.add_attribs(pp, title(soup, pp), nh1, h1, language(soup, pp),
                   mod_date(soup, pp), page_type(soup, pp), cl)

    page_time = (time.time() - start_time) / page_num
    togo_time = int((num_pages - page_num) * page_time)
    if page_num % 250 == 0:
        print(f'togo: {num_pages - page_num} pages / '
              f'{togo_time // 60}:{togo_time % 60:02} min')

logging.info('Database enrichment completed')
