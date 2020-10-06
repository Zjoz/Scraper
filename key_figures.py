"""Generate key figures for scrape range (version 1.0)."""

import sqlite3
import logging
from pathlib import Path
from scraper_lib import ScrapeDB, setup_file_logging

# TODO: dv/bib/rest pages
# TODO: pages without h1 or title
# TODO: pages with more than one h1
# TODO: internal and external links?

# ============================================================================ #
min_timestamp = '200831-0000'   # scrapes before are not processed
max_timestamp = '201006-2359'   # scrapes after are not processed
within_bd = False               # True when running on the DWB
# ============================================================================ #

# establish master scrape directory
if within_bd:
    master_dir = Path('C:/Users', 'diepj09', 'Documents/scrapes')
else:
    master_dir = Path('/home/jos/bdscraper/scrapes')
dirs = sorted([d for d in master_dir.iterdir() if d.is_dir()])
mdb_file = master_dir / 'scrape_master.db'
mdb_conn = sqlite3.connect(mdb_file, isolation_level=None)
mdb_cur = mdb_conn.cursor()
mdb_exe = mdb_cur.execute

# cycle over all scrape directories
for scrape_dir in dirs:
    sdb_file = scrape_dir / 'scrape.db'
    if not sdb_file.exists():
        # directory does not contain a scrape database; get next dir
        continue
    sdb = ScrapeDB(sdb_file, create=False)
    timestamp = sdb.get_par('timestamp')
    if timestamp <= min_timestamp or timestamp >= max_timestamp:
        # scrape is not within timestamp range; get next one
        sdb.close()
        continue

    setup_file_logging(str(scrape_dir), log_level=logging.INFO)

    # actions for a scrape start here
    # -------------------------------

    kf_qry = f'''INSERT OR REPLACE INTO key_figures
                     (timestamp, name, value) 
                 VALUES ('{timestamp}', ?, ?)'''

    # total number of pages
    mdb_exe(kf_qry, ['pages', sdb.num_pages()])

    # number of pages per language
    qry = '''SELECT language, count(*)
             FROM pages_full 
             GROUP BY language
             ORDER BY language DESC'''
    pages_lang = sdb.exe(qry).fetchall()
    for language, count in pages_lang:
        mdb_exe(kf_qry, [f'pages_lang_{language}', count])

    # number of pages per business
    qry = '''SELECT business, count(*)
             FROM pages_full 
             GROUP BY business
             ORDER BY business'''
    pages_buss = sdb.exe(qry).fetchall()
    for business, count in pages_buss:
        mdb_exe(kf_qry, [f'pages_buss_{business}', count])

    # total number of redirects
    mdb_exe(kf_qry, ['redirs', sdb.num_redirs()])

    # number of redirects per type
    qry = '''SELECT type, count(*)
             FROM redirs
             GROUP BY type'''
    types_count = sdb.exe(qry).fetchall()
    for rt, count in types_count:
        mdb_exe(kf_qry, [f'redirs_{rt}', count])

    # TODO: watch it! in older db's 301/302 types are named 'server'
    # url's with aliases
    qry = '''SELECT alias_per_url, count(alias_per_url)
             FROM (SELECT redir_path, count(*) as alias_per_url
                   FROM redirs
                   WHERE type = 'alias'
                   GROUP BY redir_path)
             GROUP BY alias_per_url'''
    alias_nums = sdb.exe(qry).fetchall()
    for alias_per_url, count in alias_nums:
        mdb_exe(kf_qry, [f'redirs_alias_{alias_per_url}x', count])

    # number of redirects that only add or loose the last slash
    qry = '''SELECT type, count(*)
             FROM redirs
             WHERE req_path || '/' = redir_path or req_path = redir_path || '/'
             GROUP BY type'''
    slash_redirs = sdb.exe(qry).fetchall()
    for redir_type, count in slash_redirs:
        mdb_exe(kf_qry, [f'redirs_{redir_type}_slash', count])

    # -----------------------------
    # actions for a scrape end here

    sdb.close()

mdb_conn.close()
