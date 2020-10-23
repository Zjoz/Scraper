import sqlite3
import logging
import time
from pathlib import Path
from bs4 import BeautifulSoup

from scraper_lib import ScrapeDB, setup_file_logging

# ============================================================================ #
min_timestamp = '200819-0000'   # scrapes before are not processed
max_timestamp = '201018-2359'   # scrapes after are not processed
within_bd = False               # True when running on the DWB
# ============================================================================ #

# establish scrape directories
if within_bd:
    master_dir = Path('C:/Users', 'diepj09', 'Documents/scrapes')
else:
    master_dir = Path('/home/jos/bdscraper/scrapes')
dirs = sorted([d for d in master_dir.iterdir() if d.is_dir()])

# cycle over all relevant scrape directories
for scrape_dir in dirs:
    dbo_file = scrape_dir / 'scrape.db'
    if not dbo_file.exists():
        # directory does not contain a scrape database; get next dir
        continue
    dbo = sqlite3.connect(dbo_file, isolation_level=None)
    timestamp = dbo.execute(
        'SELECT value FROM parameters WHERE name = "timestamp"').fetchone()[0]
    if timestamp <= min_timestamp or timestamp >= max_timestamp:
        # scrape is not within timestamp range; get next one
        dbo.close()
        continue
    setup_file_logging(scrape_dir, log_level=logging.INFO)
    db_version = dbo.execute(
        'SELECT value FROM parameters WHERE name = "db_version"').fetchone()[0]
    if db_version != '2.4':
        logging.info(f'Database v{db_version} can not be converted to v2.5\n')
        dbo.close()
        continue

    logging.info('Database conversion to v2.5 started')

    # rename old db and reconnect
    dbo.close()
    dbo_file = dbo_file.rename(scrape_dir / 'scrape.v24.db')
    logging.info('Database v2.4 saved as "scrape.v24.db"')

    # create and connect new db
    dbn_path = scrape_dir / 'scrape.db'
    dbn = ScrapeDB(dbn_path, create=True)

    # attach old db
    dbn.exe('ATTACH DATABASE ? AS old', [str(dbo_file)])

    # copy parameters table
    for name, value in dbn.exe('SELECT name, value FROM old.parameters'):
        if name == 'db_version':
            continue
        dbn.upd_par(name, value)
    logging.info('Table parameters converted to db v2.5')

    # copy pages table
    dbn.exe('INSERT INTO main.pages SELECT * FROM old.pages')
    logging.info('Table pages copied to db v2.5')

    # copy redirs table
    dbn.exe('INSERT INTO main.redirs SELECT * FROM old.redirs')
    logging.info('Table redirs copied to db v2.5')

    # create new pages_info table (description field will be added)
    fields = dbn.extracted_fields + dbn.derived_fields
    columns = ', '.join([f'{f[0]} {f[1]}' for f in fields])
    dbn.exe(f'''
        CREATE TABLE pages_info (
            page_id	 INTEGER PRIMARY KEY NOT NULL UNIQUE,
            {columns},
            FOREIGN KEY (page_id)
            REFERENCES pages (page_id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT)''')
    dbn.exe('''
        CREATE VIEW pages_full AS
            SELECT *
            FROM pages
            LEFT JOIN pages_info USING (page_id)''')

    # copy fields from old pages_info table into the new one
    qry = "PRAGMA old.table_info('pages_info')"
    fields = [r[1] for r in dbn.exe(qry).fetchall()]
    fields_str = ', '.join(fields)
    dbn.exe(f'''
        INSERT INTO main.pages_info ({fields_str})
            SELECT {fields_str}
            FROM old.pages_info''')

    # add description field for all pages
    num_pages = dbn.num_pages()
    start_time = time.time()
    page_num = 0
    for page_id, page_path, page_string in dbn.pages():
        page_num += 1
        soup = BeautifulSoup(page_string, features='lxml')
        desc_tag = soup.head.find(attrs={'name': 'description'})
        if not desc_tag:
            description = None
        else:
            description = desc_tag['content']
        dbn.exe('UPDATE pages_info SET description = ? WHERE page_id = ?',
                [description, page_id])
        if page_num % 500 == 0:
            page_time = (time.time() - start_time) / page_num
            togo_time = int((num_pages - page_num) * page_time)
            print(
                f'adding description field to scrape database of {timestamp} '
                f'- togo: {num_pages - page_num} pages / '
                f'{togo_time // 60}:{togo_time % 60:02} min')

    logging.info(
        'Table pages_info copied to db v2.5 while adding descriptin field')

    # copy Links table
    dbn.exe('INSERT INTO main.links SELECT * FROM old.links')
    logging.info('Tables links copied to db v2.5')

    dbn.exe('VACUUM')
    dbn.close()

    logging.info('Database conversion to v2.5 concluded\n')
