import sqlite3
import os
import logging
import zlib
from bs4 import BeautifulSoup

from scraper_lib import ScrapeDB, setup_file_logging

# ============================================================================ #
timestamp = '200924-1711'   # determines the scrape that is used
within_bd = False           # True when running on the DWB
# ============================================================================ #

if within_bd:
    scrape_dir = 'C:\\Users\\diepj09\\Documents\\scrapes\\'
else:
    scrape_dir = '/home/jos/bdscraper/scrapes/'
scrape_dir += timestamp + ' - bd-scrape'

setup_file_logging(scrape_dir, log_level=logging.DEBUG)
logging.info('Upgrade database to version 2.2 started')

# save old db
dbo_path = os.path.join(scrape_dir, 'scrape.v21.db')
dbn_path = os.path.join(scrape_dir, 'scrape.db')
os.rename(dbn_path, dbo_path)
logging.info(f'Database version 2.1 saved as "scrape.v21.db".')

dbo_con = sqlite3.connect(dbo_path, isolation_level=None)
dbo = dbo_con.execute

# setup connection to new db
dbn = ScrapeDB(dbn_path, create=True)

# update parameters table
for name, value in dbo('SELECT name, value FROM parameters'):
    if name == 'db_version' or name == 'num_pages':
        continue
    dbn.upd_par(name, value)
logging.info('Content of v2.1 parameters table copied to db v2.2')


# while copying pages remove redundant pages from url aliases
page_num = 0
for path, doc in dbo('SELECT path, doc FROM pages'):
    page_num += 1
    soup = BeautifulSoup(zlib.decompress(doc).decode(), features='lxml')
    meta_tag = soup.head.find(
        'meta', attrs={'name': 'DCTERMS.identifier'})
    if meta_tag:
        def_url = meta_tag['content']
        if def_url.startswith('/wps/wcm/connect'):
            def_path = def_url[16:]
            if def_path != path:
                dbn.add_redir(path, def_path, 'alias_url')
        else:
            logging.warning('Non-standard definitive url; '
                            f'falling back to: {path}')
            def_path = path
    else:
        logging.error(
            f'Page without definitive url; falling back to: {path}')
        def_path = path

    # has the page been saved yet?
    page_string = dbn.get_page(def_path)
    if not page_string:
        # store page (doc is still compressed, so do not use dbn.add_redir)
        dbn.exe('INSERT INTO pages (path, doc) VALUES (?, ?)', [def_path, doc])

    if page_num % 250 == 0:
        print(f'{page_num} pages converted')

logging.info('Content of v2.1 pages table copied to v2.2 pages table; '
             'removed redundant pages caused by alias url\'s')

# add content of old redirs table to new one
for redir in dbo('SELECT req_path, resp_path, type FROM redirs'):
    dbn.add_redir(*redir)

logging.info('Table redirs converted to v2.2')

dbn.exe('VACUUM')

# log some key figures
num_pages_o = dbo('SELECT count(*) FROM pages').fetchone()[0]
num_redirs_o = dbo('SELECT count(*) FROM redirs').fetchone()[0]
logging.info('Table sizes v2.1 are:')
logging.info(f'    pages: {num_pages_o} rows')
logging.info(f'    redirs: {num_redirs_o} rows')

logging.info('Table sizes v2.2 are:')
logging.info(f'    pages: {dbn.num_pages()} rows')
logging.info(f'    redirs: {dbn.num_redirs()} rows')

logging.info('Upgrade database to version 2.2 concluded')

dbo_con.close()
dbn.close()
