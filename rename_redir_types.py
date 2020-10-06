"""Rename the redirect types in existing scrape databases."""

import logging
from pathlib import Path

from scraper_lib import ScrapeDB, setup_file_logging

# ============================================================================ #
min_timestamp = '200901-0000'   # scrapes before are not processed
max_timestamp = '201006-2359'   # scrapes after are not processed
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
    for req_path, redir_path, redir_type in db.redirs():
        qry = 'UPDATE redirs SET type = ? WHERE req_path = ?'
        db.exe(qry, [trans[redir_type], req_path])
    logging.info('Redirect types normalised\n')

    db.close()
