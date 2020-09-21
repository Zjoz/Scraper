"""Extract data from a stored scrape of www.belastingdienst.nl (version 2.3).

This module basically is a working code base for extracting info from stored
scrapes. Since the real labour is done in the classes and functions of the
scraper_lib module, the code can stay at a rather high level to keep a clear
view on the workflow.

The results of the extraction is saved in tabular form via the DataSheet class
in spreadsheets.
"""

import time
import os.path
import logging
from bs4 import BeautifulSoup

from scraper_lib import ScrapeDB, DataSheet, setup_file_logging
from scraper_lib import title, h1s, text, links

# ============================================================================ #
timestamp = '200918-1541'   # determines the scrape that is used
within_bd = False           # True when running on the DWB
# ============================================================================ #

# establish directory of the scrape
if within_bd:
    scrapes_dir = 'C:\\Users\\diepj09\\Documents\\scrapes\\'
else:
    scrapes_dir = '/home/jos/bdscraper/scrapes/'
scrape_dir = scrapes_dir + timestamp + ' - bd-scrape'

# connect to the scrape database
db_file = os.path.join(scrape_dir, 'scrape.db')
db = ScrapeDB(db_file, create=False)

setup_file_logging(scrape_dir, log_level=logging.INFO)

# get some parameters from the scrape
root_url = db.get_par('root_url')
num_pages = db.num_pages()

# prepare to save data to worksheets
pages_ds = DataSheet('Pages', ('Path', 55), ('Title', 55), ('First h1', 55),
                     ("# h1's", 10), ('Page text', 55))
links_ds = DataSheet('Links', ('Page path', 85), ('Link text', 55),
                     ('Link destination', 85))

start_time = time.time()
logging.info('Site data extraction started')

page_num = 0
for page_path, page_string in db.pages():
    page_num += 1
    page_soup = BeautifulSoup(page_string, features='lxml')
    page_h1s = h1s(page_soup, page_path)
    pages_ds.append((page_path, title(page_soup, page_path), page_h1s[0],
                     len(page_h1s), text(page_soup)))

    for link_text, link_url in links(page_soup, root_url,
                      root_rel=True, excl_hdr_ftr=True, remove_anchor=True):
        links_ds.append((page_path, link_text, link_url))

    page_time = (time.time() - start_time) / page_num
    togo_time = int((num_pages - page_num) * page_time)
    if page_num % 100 == 0:
        print(f'togo: {num_pages - page_num} pages / '
              f'{togo_time // 60}:{togo_time % 60:02} min')

pages_ds.save(os.path.join(scrape_dir, 'pages.xlsx'))
logging.info('Spreadsheet pages.xlsx saved to scrape directory')
links_ds.save(os.path.join(scrape_dir, 'links.xlsx'))
logging.info('Spreadsheet links.xlsx saved to scrape directory')

# save redirects in a spreadsheet
redirs_ds = DataSheet('Redirs', ('Requested path', 110),
                      ('Redirected path', 110), ('Type', 10))
for redir in db.redirs():
    redirs_ds.append(redir)
redirs_ds.save(os.path.join(scrape_dir, 'redirs.xlsx'))
logging.info('Spreadsheet redirs.xlsx saved to scrape directory')

logging.info('Site data extraction completed')
