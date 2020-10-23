"""Generate key and dimensionsl figures for scrape range (version 1.2).

Key and dimensional figures will be generated for all the scrapes within the
given range and (re)written to the scrape_master database.
"""

import sqlite3
from pathlib import Path
from scraper_lib import ScrapeDB

# ============================================================================ #
min_timestamp = '200119-0000'  # scrapes before are not processed
max_timestamp = '201019-2359'  # scrapes after are not processed
within_bd = False  # True when running on the DWB
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

    # actions for a scrape start here
    # -------------------------------

    kf_qry = f'''
        INSERT OR REPLACE INTO key_figures
            (timestamp, name, value) 
        VALUES ('{timestamp}', ?, ?)'''

    # total pages
    mdb_exe(kf_qry, ['pages', sdb.num_pages()])

    # pages per language
    qry = '''
        SELECT language, count(*)
        FROM pages_info 
        GROUP BY language
        ORDER BY language DESC'''
    pages_lang = sdb.exe(qry).fetchall()
    for language, count in pages_lang:
        mdb_exe(kf_qry, [f'pages_lang_{language}', count])

    # pages per business
    qry = '''
        SELECT business, count(*)
        FROM pages_info 
        GROUP BY business
        ORDER BY business'''
    pages_buss = sdb.exe(qry).fetchall()
    for business, count in pages_buss:
        mdb_exe(kf_qry, [f'pages_buss_{business}', count])

    # pages per category
    qry = '''
        SELECT category, count(*)
        FROM pages_info 
        GROUP BY category
        ORDER BY business DESC'''
    pages_cat = sdb.exe(qry).fetchall()
    for category, count in pages_cat:
        mdb_exe(kf_qry, [f'pages_cat_{category}', count])

    # pages per type
    qry = '''
        SELECT pagetype, count(*)
        FROM pages_info
        GROUP BY pagetype
        ORDER BY category DESC, count(*) ASC'''
    types_count = sdb.exe(qry).fetchall()
    for pagetype, count in types_count:
        mdb_exe(kf_qry, [f'pages_type_{pagetype}', count])

    # total redirects (an alias is strictly no redirect)
    qry = "SELECT count(*) FROM redirs WHERE type != 'alias'"
    mdb_exe(kf_qry, ['redirs', sdb.exe(qry).fetchone()[0]])

    # redirects per type
    qry = '''
        SELECT type, count(*)
        FROM redirs
        WHERE type != 'alias'
        GROUP BY type'''
    types_count = sdb.exe(qry).fetchall()
    for redir_type, count in types_count:
        mdb_exe(kf_qry, [f'redirs_{redir_type}', count])

    # redirects per type that only add or loose the last slash
    qry = '''
        SELECT type, count(*)
        FROM redirs
        WHERE req_path || '/' = redir_path or req_path = redir_path || '/'
        GROUP BY type'''
    slash_redirs = sdb.exe(qry).fetchall()
    for redir_type, count in slash_redirs:
        mdb_exe(kf_qry, [f'redirs_{redir_type}_slash', count])

    # total aliases
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

    # pages with more than one h1's
    qry = '''
        SELECT count(*)
        FROM pages_info
        WHERE num_h1s > 1'''
    mdb_exe(kf_qry, ['pages_multi-h1', sdb.exe(qry).fetchone()[0]])

    # pages per type with more than one h1's
    qry = '''
        SELECT pagetype, count(*)
        FROM pages_info
        WHERE num_h1s > 1
        GROUP BY pagetype'''
    multi_h1s = sdb.exe(qry).fetchall()
    for pagetype, count in multi_h1s:
        mdb_exe(kf_qry, [f'pages_multi-h1_{pagetype}', count])

    # pages with no h1
    qry = '''
        SELECT count(*) 
        FROM pages_info 
        WHERE num_h1s = 0'''
    mdb_exe(kf_qry, ['pages_no-h1', sdb.exe(qry).fetchone()[0]])

    # pages without title
    qry = '''
        SELECT count(*) 
        FROM pages_info 
        WHERE title = '' or title is NULL'''
    mdb_exe(kf_qry, ['pages_no-title', sdb.exe(qry).fetchone()[0]])

    # pages with non unique title
    qry = '''
        SELECT sum(c) FROM 
            (SELECT count(*) as c
            FROM pages_info
            GROUP BY title)
        WHERE c > 1'''
    mdb_exe(kf_qry, ['pages_dupl-title', sdb.exe(qry).fetchone()[0]])

    # pages without description
    qry = '''
        SELECT count(*) 
        FROM pages_info 
        WHERE description = '' or description is NULL'''
    mdb_exe(kf_qry, ['pages_no-descr', sdb.exe(qry).fetchone()[0]])

    # pages with description longer than 160 characters
    qry = '''
        SELECT count(*) 
        FROM pages_info 
        WHERE length(description) > 160'''
    mdb_exe(kf_qry, ['pages_long-descr', sdb.exe(qry).fetchone()[0]])

    # dimensional figures
    qry = '''
        SELECT language, business, category, pagetype, count(*)
        FROM pages_full
        GROUP BY language, business, category, pagetype
        ORDER BY language DESC, business, category DESC, count(*) ASC'''
    dim_qry = f'''
        INSERT OR REPLACE INTO dimensions
            (timestamp, language, business, category, pagetype, pages)
        VALUES ('{timestamp}', ?, ?, ?, ?, ?)'''
    for values in sdb.exe(qry).fetchall():
        values = ['' if v is None else v for v in values]
        mdb_exe(dim_qry, values)

    # -----------------------------
    # actions for a scrape end here

    sdb.close()

    print(f'Typical figures saved to master database for scrape of {timestamp}')

mdb_conn.close()
