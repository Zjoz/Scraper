"""Scrape www.belastingdienst.nl and store the results (version 2.6).

Since scraping is not always possible from within the belastingdienst
organisation, this module is supposed to run on a private pc with an open
internet connection. Running this module will create a directory (at the
location from where this module is executed) named '<date-and-timestamp> -
bd-scrape'. After the scrape has finished this directory will contain the
next files:

    'scrape.db': SQLite database with the results of the scrape
    'scrape.db-<nn>.txt': parts of scrape.db for text-based transmission
    'log.txt': a scrape log with info, warnings and/or errors of the scrape

Depending on the actual value of the (bool) parameter 'publish', the directory
will be moved to the publication destination (actual value of 'publ_dir'
parameter).

The scrape database contains the next tables (all paths are relative to the
root_url of a scrape):

    table parameters, with columns:
        name (text): name of the parameter
        value (text): value of the parameter

    table pages, with columns:
        page_id (integer): key to a specific page
        path (text): path of a page
        doc (zlib compressed utf-8 encoded text): complete response from the
            page-request for later extraction of data and other information

    table redirs, with columns:
        redir_id (integer): key to a specific redirect
        req_path (text): requested path
        redir_path (text): path to where the request was directed
        type (text): nature of the redirect

    When parameter add_info is True, the next table and view are also created:

    table pages_info, with columns
        page_id (integer): page_id, key into pages table
        path (text): path of page
        title (text): page title
        num_h1s (integer): number of <h1> tags
        first_h1 (text): text of the first <h1> tag
        language (text): language
        modified (date): last modification date
        pagetype (text): page type
        classes (text): classes of the page separated by spaces

    view pages_full,
        a join of all columns from the pages and pages_info table
"""

# TODO: add silent mode
# TODO: change starting parameters into command line arguments and options

import shutil
import os
import time
import re
import logging
from requests import RequestException

from scraper_lib import ScrapeDB, setup_file_logging
from scraper_lib import scrape_page, links, valid_path, populate_links_table
from scraper_lib import extract_info, derive_info
from bd_viauu import bintouu, split_uufile

# ============================================================================ #
root_url = 'https://www.belastingdienst.nl/wps/wcm/connect'
start_path = '/nl/home'
max_paths = 15000           # total some 10000 actual (paths, not pages)
do_links_table = True       # populate links table
do_extract_info = True      # add extracted_info table
do_derive_info = True       # add derived_info table (only when also extract)
publish = True
publ_dir = '/var/www/bds/scrapes'
# ============================================================================ #

# setup output and database
timestamp = time.strftime('%y%m%d-%H%M')
dir_name = timestamp + ' - bd-scrape'
os.mkdir(dir_name)
publ_path = os.path.join(publ_dir, dir_name)
db_file = os.path.join(dir_name, 'scrape.db')
db = ScrapeDB(db_file, create=True)
db.upd_par('root_url', root_url)
db.upd_par('start_path', start_path)
db.upd_par('timestamp', timestamp)

# setup logging; all log messages go to file, console receives warnings and
# higher severity messages
setup_file_logging(dir_name, log_level=logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(
    logging.Formatter('%(levelname)-8s - %(message)s'))
logging.getLogger('').addHandler(console_handler)

# initialize some variables
paths_todo = {start_path}
paths_done = set()
num_done = 0

start_time = time.time()
logging.info('Site scrape started')
logging.info(f'    root_url: {root_url}')
logging.info(f'    start_path: {start_path}')

while paths_todo and num_done < max_paths:

    # scrape page
    req_path = paths_todo.pop()
    try:
        req_url = root_url + req_path
        def_url, soup, string_doc, redirs = scrape_page(root_url, req_url)
    except RequestException:
        # handled and logged in scrape_page function; we consider this one done
        paths_done.add(req_path)
        continue

    # if in scope, save page to db under the definitive path
    def_url_parts = def_url.split(root_url)
    if not def_url_parts[0]:
        # url is within scope
        def_path = def_url_parts[1]
        page_id = db.add_page(def_path, string_doc)

    # update paths_done admin and save redirects to db
    if redirs:
        for req_url, red_url, redir_type in redirs:
            req_path = re.sub(root_url, '', req_url)
            red_path = re.sub(root_url, '', red_url)
            if req_path.startswith('/'):
                paths_done.add(req_path)
            if red_path.startswith('/'):
                paths_done.add(red_path)
            redir_id = db.add_redir(req_path, red_path, redir_type)
    else:
        paths_done.add(req_path)
    num_done += 1

    # add relevant links to paths_todo list (include links from header and
    # footer to trace all pages)
    for l_text, l_path in links(soup, root_url, root_rel=True,
                                excl_hdr_ftr=False, remove_anchor=True):
        if l_path.startswith('/'):
            # link within scope
            if l_path not in (paths_todo | paths_done) and valid_path(l_path):
                paths_todo.add(l_path)

    # time cycles and print progress and prognosis
    num_todo = min(len(paths_todo), max_paths - num_done)
    if num_done % 25 == 0:
        page_time = (time.time() - start_time) / num_done
        togo_time = int(num_todo * page_time)
        print(f'{num_done:4} done, {page_time:.2f} sec per page / {num_todo:4} '
              f'todo, {togo_time//60}:{togo_time % 60:02} min togo')

elapsed = int(time.time() - start_time)
logging.info(f'Site scrape finished in {elapsed//60}:{elapsed % 60:02} min')
logging.info(f'    pages: {db.num_pages()}')
logging.info(f'    redirs: {db.num_redirs()}\n')

if do_links_table:
    populate_links_table(db)

if do_extract_info:
    extract_info(db)
    if do_derive_info:
        derive_info(db)

db.close()

if publish:
    # prepare database for publication
    uu_file = db_file + '.uu'
    bintouu(db_file, uu_file)
    split_uufile(uu_file, max_mb=30)
    os.remove(uu_file)

    # publish results
    shutil.move(dir_name, publ_path)
