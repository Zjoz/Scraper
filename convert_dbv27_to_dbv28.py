import sqlite3
import logging
import time
from pathlib import Path
from bs4 import BeautifulSoup

from scraper_lib import ScrapeDB, setup_file_logging, scrape_dirs, get_text

# ============================================================================ #
min_timestamp = '200901-0000'   # scrapes before are not processed
max_timestamp = '201116-2359'   # scrapes after are not processed
within_bd = False               # True when running on the DWB
v_old = '2.7'                   # old db version
v_new = '2.8'                   # new db version
# ============================================================================ #

# establish scrape directories
if within_bd:
    master_dir = Path('C:/Users', 'diepj09', 'Documents/scrapes')
else:
    master_dir = Path('/home/jos/bdscraper/scrapes')
dirs = sorted([d for d in master_dir.iterdir() if d.is_dir()])

# cycle over all relevant scrape directories
for timestamp, scrape_dir in scrape_dirs(master_dir, min_timestamp,
                                         max_timestamp, check=False):
    dbo_file = scrape_dir / 'scrape.db'
    dbo = sqlite3.connect(dbo_file, isolation_level=None)
    setup_file_logging(scrape_dir, log_level=logging.INFO)
    db_version = dbo.execute(
        'SELECT value FROM parameters WHERE name = "db_version"').fetchone()[0]
    if db_version != v_old:
        logging.info(
            f'Database v{db_version} can not be converted to v{v_new}')
        dbo.close()
        continue

    logging.info(f'Database conversion to v{v_new} started')

    # rename old db and reconnect
    dbo.close()
    dbo_file = dbo_file.rename(
        scrape_dir / f'scrape.v{v_old.replace(".", "")}.db')
    logging.info(
        f'Database v{v_old} saved as "scrape.v{v_old.replace(".", "")}.db"')

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
    logging.info(f'Table parameters converted to db v{v_new}')

    # copy pages table
    dbn.exe('INSERT INTO main.pages SELECT * FROM old.pages')
    logging.info(f'Table pages copied to db v{v_new}')

    # copy redirs table
    dbn.exe('INSERT INTO main.redirs SELECT * FROM old.redirs')
    logging.info(f'Table redirs copied to db v{v_new}')

    # create new pages_info table (aut_content will added)
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

    # copy all fields except ed_text from old pages_info table into the new one
    qry = "PRAGMA old.table_info('pages_info')"
    fields = [r[1] for r in dbn.exe(qry).fetchall() if r[1] != 'ed_text']
    fields_str = ', '.join(fields)
    dbn.exe(f'''
        INSERT INTO main.pages_info ({fields_str})
            SELECT {fields_str}
            FROM old.pages_info''')

    # extract ed_text and aut_text from all pages and add to pages_info table
    num_pages = dbn.num_pages()
    start_time = time.time()
    page_num = 0
    for page_id, page_path, page_string in dbn.pages():
        page_num += 1
        soup = BeautifulSoup(page_string, features='lxml')
        ed_text, aut_text = get_text(soup)
        dbn.exe('''
            UPDATE pages_info
            SET ed_text = ?,
                aut_text = ?
            WHERE page_id = ?''',
                [ed_text, aut_text, page_id])
        if page_num % 250 == 0:
            page_time = (time.time() - start_time) / page_num
            togo_time = int((num_pages - page_num) * page_time)
            print(
                f'updating ed_text and inserting aut_text to scrape database '
                f'of {timestamp} - togo: {num_pages - page_num} pages / '
                f'{togo_time // 60}:{togo_time % 60:02} min')

    logging.info(f'Table pages_info copied to db v{v_new}, '
                 f'while updating ed_text and adding aut_text fields')

    # repopulate links table
    dbn.repop_ed_links()

    dbn.exe('VACUUM')
    dbn.close()

    logging.info(f'Database conversion to v{v_new} concluded')
    print(f'database conversion of {timestamp} concluded')
