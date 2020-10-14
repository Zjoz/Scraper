import sqlite3
import logging
import zlib
from pathlib import Path

from scraper_lib import ScrapeDB, setup_file_logging, populate_links_table

# ============================================================================ #
min_timestamp = '200928-0000'   # scrapes before are not processed
max_timestamp = '201006-2359'   # scrapes after are not processed
links_table = True              # populate links table
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
    setup_file_logging(str(scrape_dir), log_level=logging.INFO)
    db_version = dbo.execute(
        'SELECT value FROM parameters WHERE name = "db_version"').fetchone()[0]
    if db_version != '2.2':
        logging.info(f'Database v{db_version} can not be converted to v2.3\n')
        dbo.close()
        continue

    logging.info('Database conversion to v2.3 started')

    # rename old db and reconnect
    dbo.close()
    dbo_file = dbo_file.rename(scrape_dir / 'scrape.v22.db')
    logging.info('Database v2.2 saved as "scrape.v22.db"')
    dbo_con = sqlite3.connect(dbo_file, isolation_level=None)
    dbo = dbo_con.execute

    # create and connect new db
    dbn_path = scrape_dir / 'scrape.db'
    dbn = ScrapeDB(dbn_path, create=True)

    # copy parameters table
    for name, value in dbo('SELECT name, value FROM parameters'):
        if name == 'db_version':
            continue
        dbn.upd_par(name, value)
    logging.info('Table parameters converted to db v2.3')

    # copy pages table
    page_num = 0
    for path, doc in dbo('SELECT path, doc FROM pages'):
        page_num += 1
        doc = zlib.decompress(doc).decode()
        dbn.add_page(path, doc)
        if page_num % 500 == 0:
            print(f'[{timestamp}] - {page_num} pages copied')
    logging.info('Table pages copied to db v2.3')

    # copy pages_info to extracted_info without column business
    # creates pages_extra view as well (do not copy pages_full view)
    dbn.new_extracted_info_table()
    page_num = 0
    for info in dbo('SELECT path, title, num_h1s, first_h1, language, modified,'
                    'pagetype, classes FROM pages_full'):
        page_num += 1
        # get page_id from new pages table
        page_id = dbn.exe('SELECT page_id FROM pages WHERE path = ?',
                          [info[0]]).fetchone()[0]
        info = list(info)
        info[0] = page_id
        dbn.exe('INSERT INTO extracted_info VALUES (?,?,?,?,?,?,?,?)', info)
        if page_num % 500 == 0:
            print(f'[{timestamp}] - {page_num} pages info copied')
    logging.info('Table pages_info copied to extracted_info in db v2.3')

    # copy redirs table
    trans = {
        'alias url': 'alias',
        'alias_url': 'alias',
        'alias': 'alias',
        'client-side refresh': 'client',
        'client-side': 'client',
        'client': 'client',
        'server-side': 'server',
        'server': 'server',
        'redir 301': '301',
        'redir 302': '302',
        '301': '301',
        '302': '302'
    }
    redir_num = 0
    qry = 'SELECT req_path, redir_path, type FROM redirs'
    for req_path, redir_path, redir_type in dbo(qry):
        redir_num += 1
        dbn.add_redir(req_path, redir_path, trans[redir_type])
        if redir_num % 500 == 0:
            print(f'[{timestamp}] - {redir_num} redirs copied')
    logging.info(
        'Table redirs copied to db v2.3, while normalising redir types')

    # populate links table
    if links_table:
        dbn.purge_links_table()
        populate_links_table(dbn)

    dbn.exe('VACUUM')
    dbo_con.close()
    dbn.close()

    logging.info('Database conversion to v2.3 concluded\n')
