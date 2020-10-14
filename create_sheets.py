"""Extract data to spreadsheets for a range of stored scrapes (version 2.5).

Since the real labour is done in the classes and functions of the
scraper_lib module, the code can stay at a rather high level to keep a clear
view on the workflow.

The results of the extraction is saved in tabular form via the DataSheet class
in spreadsheets.
"""

import time
import logging
from pathlib import Path
from bs4 import BeautifulSoup

from scraper_lib import ScrapeDB, DataSheet, setup_file_logging
from scraper_lib import extract_info, derive_info, text

# ============================================================================ #
min_timestamp = '201012-0000'   # scrapes before are not processed
max_timestamp = '201012-2359'   # scrapes after are not processed
recre_extract_info = False      # recreates extracted_info table
recre_derive_info = False       # recreates derived_info table
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

    # get some parameters from the scrape
    root_url = db.get_par('root_url')
    num_pages = db.num_pages()

    # (re)creates the info tables and views in the database
    if recre_extract_info:
        extract_info(db)
    if recre_derive_info:
        derive_info(db)

    # export pages info to a spreadsheet
    pages_ds = DataSheet('Pages', ('Path', 55), ('Title', 35), ('First h1', 35),
                         ("# h1's", 9), ('Language', 12), ('Modified', 15),
                         ('Page type', 20), ('Classes', 25), ('Business', 20),
                         ('Category', 12), ('Page text', 55))
    start_time = time.time()
    logging.info('Site data extraction started')
    page_num = 0
    for info in db.pages_full():
        page_num += 1
        soup = BeautifulSoup(info['doc'], features='lxml')
        pages_ds.append((info['path'], info['title'], info['first_h1'],
                         info['num_h1s'], info['language'], info['modified'],
                         info['pagetype'], info['classes'], info['business'],
                         info['category'], text(soup)))

        page_time = (time.time() - start_time) / page_num
        togo_time = int((num_pages - page_num) * page_time)
        if page_num % 100 == 0:
            print(f'creating sheets for scrape of {timestamp} - togo: '
                  f'{num_pages - page_num} pages / '
                  f'{togo_time // 60}:{togo_time % 60:02} min')

    pages_ds.save(str(scrape_dir / 'pages.xlsx'))
    logging.info('Spreadsheet pages.xlsx saved to scrape directory')

    # export links to a spreadsheet
    links_ds = DataSheet('Links', ('Page path', 70), ('Link text', 50),
                         ('Link path', 70), ('Link url', 70))
    for link_info in db.links_expl():
        links_ds.append(link_info)
    links_ds.save(str(scrape_dir / 'links.xlsx'))
    logging.info('Spreadsheet links.xlsx saved to scrape directory')

    # export redirects to a spreadsheet
    redirs_ds = DataSheet('Redirs', ('Requested path', 110),
                          ('Redirected path', 110), ('Type', 10))
    for redir in db.redirs():
        redirs_ds.append(redir)
    redirs_ds.save(str(scrape_dir / 'redirs.xlsx'))
    logging.info('Spreadsheet redirs.xlsx saved to scrape directory')

    db.close()

    logging.info('Site data extraction completed\n')
