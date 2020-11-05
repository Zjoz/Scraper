import sqlite3
import logging
from pathlib import Path

from scraper_lib import ScrapeDB, setup_file_logging

# ============================================================================ #
min_timestamp = '201102-0000'   # scrapes before are not processed
max_timestamp = '201102-2359'   # scrapes after are not processed
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
    if db_version != '2.6':
        logging.info(f'Database v{db_version} can not be converted to v2.7\n')
        dbo.close()
        continue

    logging.info('Database conversion to v2.7 started')

    # rename old db and reconnect
    dbo.close()
    dbo_file = dbo_file.rename(scrape_dir / 'scrape.v26.db')
    logging.info('Database v2.6 saved as "scrape.v26.db"')

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
    logging.info('Table parameters converted to db v2.7')

    # copy pages table
    dbn.exe('INSERT INTO main.pages SELECT * FROM old.pages')
    logging.info('Table pages copied to db v2.7')

    # copy redirs table
    dbn.exe('INSERT INTO main.redirs SELECT * FROM old.redirs')
    logging.info('Table redirs copied to db v2.6')

    # create new pages_info table (ed_content will be renamed to ed_text)
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
    dbn.exe('INSERT INTO main.pages_info SELECT * FROM old.pages_info')
    logging.info('Table pages_info copied to db v2.7')

    # repopulate links table
    dbn.repop_ed_links()

    dbn.exe('VACUUM')
    dbn.close()

    logging.info('Database conversion to v2.7 concluded\n')
    print(f'database conversion of {timestamp} concluded')
