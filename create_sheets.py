"""Extract data from a stored scrape of www.belastingdienst.nl (version 2.3)

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
from scraper_lib import add_pages_info, text, links

# ============================================================================ #
timestamp = '200926-0937'  # determines the scrape that is used
add_info = True            # creates new pages_info table
within_bd = False          # True when running on the DWB
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

# (re)creates the pages_info table and pages_full view in the database
if add_info:
    add_pages_info(db)

# prepare to save data to worksheets
pages_ds = DataSheet('Pages', ('Path', 55), ('Title', 35), ('First h1', 35),
                     ("# h1's", 9), ('Language', 12), ('Modified', 15),
                     ('Page type', 20), ('Classes', 25), ('Page text', 55))
links_ds = DataSheet('Links', ('Page path', 85), ('Link text', 55),
                     ('Link destination', 85))

start_time = time.time()
logging.info('Site data extraction started')

page_num = 0
for info in db.pages_full():
    page_num += 1
    soup = BeautifulSoup(info['doc'], features='lxml')

    pages_ds.append((info['path'], info['title'], info['first_h1'],
                     info['num_h1s'], info['language'], info['modified'],
                     info['pagetype'], info['classes'], text(soup)))

    for link_text, link_url in links(soup, root_url,
                                     root_rel=True, excl_hdr_ftr=True,
                                     remove_anchor=True):
        links_ds.append((info['path'], link_text, link_url))

    page_time = (time.time() - start_time) / page_num
    togo_time = int((num_pages - page_num) * page_time)
    if page_num % 100 == 0:
        print(f'creating sheets - togo: {num_pages - page_num} pages / '
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
