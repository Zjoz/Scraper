"""Generate key figures for scrape range (version 1.0)."""

import sqlite3
import logging
from pathlib import Path
from scraper_lib import ScrapeDB, setup_file_logging

# TODO: internal and external links?
# TODO: what can we do with robots.txt and sitemap.xml?

# ============================================================================ #
min_timestamp = '201002-0000'   # scrapes before are not processed
max_timestamp = '201002-2359'   # scrapes after are not processed
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

    kf_qry = f'''
        INSERT OR REPLACE INTO key_figures
            (timestamp, name, value) 
        VALUES ('{timestamp}', ?, ?)'''

    # total number of pages
    mdb_exe(kf_qry, ['pages', sdb.num_pages()])

    # number of pages per language
    qry = '''
        SELECT language, count(*)
        FROM pages_full 
        GROUP BY language
        ORDER BY language DESC'''
    pages_lang = sdb.exe(qry).fetchall()
    for language, count in pages_lang:
        mdb_exe(kf_qry, [f'pages_lang_{language}', count])

    # number of pages per business
    qry = '''
        SELECT business, count(*)
        FROM pages_full 
        GROUP BY business
        ORDER BY business'''
    pages_buss = sdb.exe(qry).fetchall()
    for business, count in pages_buss:
        mdb_exe(kf_qry, [f'pages_buss_{business}', count])

    # number of pages per category
    qry = '''
        SELECT category, count(*)
        FROM pages_full 
        GROUP BY category
        ORDER BY business DESC'''
    pages_cat = sdb.exe(qry).fetchall()
    for category, count in pages_cat:
        mdb_exe(kf_qry, [f'pages_cat_{category}', count])

    # total number of redirects (an alias is strictly no redirect)
    qry = "SELECT count(*) FROM redirs WHERE type != 'alias'"
    mdb_exe(kf_qry, ['redirs', sdb.exe(qry).fetchone()[0]])

    # number of redirects per type
    qry = '''
        SELECT type, count(*)
        FROM redirs
        WHERE type != 'alias'
        GROUP BY type'''
    types_count = sdb.exe(qry).fetchall()
    for rt, count in types_count:
        mdb_exe(kf_qry, [f'redirs_{rt}', count])

    # number of redirects per type that only add or loose the last slash
    qry = '''
        SELECT type, count(*)
        FROM redirs
        WHERE req_path || '/' = redir_path or req_path = redir_path || '/'
        GROUP BY type'''
    slash_redirs = sdb.exe(qry).fetchall()
    for redir_type, count in slash_redirs:
        mdb_exe(kf_qry, [f'redirs_{redir_type}_slash', count])

    # total number of aliases
    qry = '''
        SELECT count(*)
        FROM redirs
        WHERE type = "alias"'''
    mdb_exe(kf_qry, ['url-aliases', sdb.exe(qry).fetchone()[0]])

    # url's with aliases
    qry = '''
        SELECT alias_per_url, count(alias_per_url)
        FROM (SELECT redir_path, count(*) as alias_per_url
            FROM redirs
            WHERE type = 'alias'
            GROUP BY redir_path)
        GROUP BY alias_per_url'''
    alias_nums = sdb.exe(qry).fetchall()
    for alias_per_url, count in alias_nums:
        mdb_exe(kf_qry, [f'url-aliases_{alias_per_url}x', count])

    # total number of pages with more than one h1's
    qry = 'SELECT count(*) FROM pages_full WHERE num_h1s > 1'
    mdb_exe(kf_qry, ['pages_multi-h1', sdb.exe(qry).fetchone()[0]])

    # pages with more than one h1's
    qry = '''
        SELECT pagetype, count(*)
        FROM pages_full
        WHERE num_h1s > 1
        GROUP BY pagetype'''
    multi_h1s = sdb.exe(qry).fetchall()
    for page_type, count in multi_h1s:
        mdb_exe(kf_qry, [f'pages_multi-h1_{page_type}', count])

    # pages with no h1
    qry = '''
        SELECT count(*) 
        FROM pages_full 
        WHERE num_h1s = 0'''
    mdb_exe(kf_qry, ['pages_no-h1', sdb.exe(qry).fetchone()[0]])

    # pages with no title
    qry = '''
        SELECT count(*) 
        FROM pages_full 
        WHERE title is NULL'''
    mdb_exe(kf_qry, ['pages_no-title', sdb.exe(qry).fetchone()[0]])

    # dimensional figures
    qry = '''
        SELECT language, business, category, pagetype, count(*)
        FROM pages_full
        GROUP BY language, business, category, pagetype
        ORDER BY language DESC, business, category DESC, count(*) ASC'''
    dim_qry = f'''
        INSERT OR REPLACE INTO dimensions
            (timestamp, language, business, category, pagetype, num_pages)
        VALUES ('{timestamp}', ?, ?, ?, ?, ?)'''
    for values in sdb.exe(qry).fetchall():
        mdb_exe(dim_qry, values)

    # -----------------------------
    # actions for a scrape end here

    sdb.close()

mdb_conn.close()
