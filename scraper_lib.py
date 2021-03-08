"""
***Library for scraping www.belastingdienst.nl (version 3.0.0).***

The main purpose of this library is to scrape the www-site of the Dutch tax
organisation. With the help of the `scrape_site.py` module a scrape can be
executed from the command line with various arguments:

    usage: scrape_site.py [options] master_dir

    Scrape www.belastingdienst.nl

    positional arguments:
      master_dir     directory with the master database and destination of the
                     scrape

    optional arguments:
      -h, --help     show this help message and exit
      -r ROOT        root path that sets the scope of the scrape (default:
                     /wps/wcm/connect)
      -s START_URLS  path to the file with url's to start the scrape (default:
                     start_urls.txt)
      -n             do not use the sitemap to start the scrape (default: False)
      -x MAX_URLS    maximum number of url's to request while scraping (default:
                     15000)
      -w             this is a weekly scrape (default: False)
      -m             this is a monthly scrape (default: False)
      -o             override date checks for weekly and monthly scrape to
                     force going into history and have a report (default: False)
      -b             perform a basic scrape only, independent of and unrelated
                     to the master database (default: False)
      -u             prepare for uu-based transmission: -u scrape database only,
                     -uu full scrape directory, -uuu zip with all changed files
                     (default: no preparation)
      -v, --version  show version of the scraper and exit


To have a basic understanding of how a scrape is executed please refer to the
documentation of the `scrape_site` function of this library.

The main design consideration for all this library is that scraping should be
able to be scheduled and run unattended, hence the command line module
referred above. Therefore no unhandled exceptions should occur and the basic
scrape, i.e. getting and storing all pages together with the redirections
that occur while retrieving them, should continue above all. """

__all__ = [
    'SCRAPER_VERSION', 'DB_VERSION', 'REPORT_VERSION',
    '_SITE', '_EXTRACTED_FIELDS', '_DERIVED_FIELDS', '_MAX_DOW', '_MAX_DOM',
    '_DV_TYPES', '_BIB_TYPES', '_ALG_TYPES',
    'scrape_site', 'create_scrape_db', 'parameter', 'prime_scrape',
    'scrape_page', 'split_tree', 'extract_pages_info', 'get_text',
    'flatten_tagbranch_to_navstring', 'derive_pages_info', 'id_tables',
    'sync_page_ids', 'key_figures', 'dimensions', 'compile_history',
    'report_scrape', 'mod_factor', 'mail_result', 'create_master_db'
]

import configparser
import copy
import difflib
import logging
import re
import requests
import shutil
import smtplib
import sqlite3
import ssl
import time
import urllib3
import xlsxwriter
import zipfile
import zlib
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag, Comment, Script, Stylesheet
from datetime import date, datetime, timedelta
from operator import itemgetter
from pathlib import Path
from urllib.parse import urljoin
from typing import Union, Set, List

from bd_viauu import bintouu, split_uufile


# Public constants
SCRAPER_VERSION = '3.0.0'
"Version of this library."
DB_VERSION = '3.0'
"Version of the database structure."
REPORT_VERSION = '1.1.0'
"Version of the scrape report."

# Non-public constants
_SITE = 'https://www.belastingdienst.nl'
"Site for which this scraper is built."
_EXTRACTED_FIELDS = [
    ('title', 'TEXT'),          # content of <title> tag
    ('description', 'TEXT'),    # content of <meta name="description"
                                #             content="..." />
    ('num_h1s', 'INTEGER'),     # number of <h1> tags
    ('first_h1', 'TEXT'),       # content of first <h1> tag
    ('language', 'TEXT'),       # content of <meta name="language"
                                #             content="xx" />
    ('modified', 'DATE'),       # content of <meta name="DCTERMS.modified"
                                #             content="date" />
    ('pagetype', 'TEXT'),       # attribute value of <body data-pageType="...">
    ('classes', 'TEXT'),        # attribute value of <body class="...">
    ('ed_text', 'TEXT'),        # editorial text of the page
    ('aut_text', 'TEXT')        # automated text of the page
]
"Field definitions for the pages_info table of the scrape database (part 1)."
_DERIVED_FIELDS = [
    ('business', 'TEXT'),       # 'belastingen', 'toeslagen' or 'douane'
    ('category', 'TEXT')        # 'dv', 'bib' or 'alg'
]
"Field definitions for the pages_info table of the scrape database (part 2)."
_MAX_DOW = 2
"""Last day-of-week on which scrapes are accepted in the weekly history of 
the master database."""
_MAX_DOM = 3
"""Last day-of-month on which scrapes are accepted in the monthly history of 
the master database."""
_DV_TYPES = {'bld-filter', 'bld-dv-content'}
"Set of page-types that belong to the `dv` category."
_BIB_TYPES = {'bld-bd', 'bld-cluster', 'bld-direction', 'bld-landing',
              'bld-overview', 'bld-sitemap', 'bld-target',
              'bld-targetGroup',
              'bld-concept', 'bld-faq'}
"Set of page-types that belong to the `bib` category."
_ALG_TYPES = {'bld-outage', 'bld-newsItem', 'bld-iahWrapper'}
"Set of page-types that belong to the `alg` category."

# Under the hood, the requests package uses the urllib3 package. While
# checking links in the scrape_page function, this package can give some
# warnings that are not relevant in the context of this scraper.
urllib3.disable_warnings()


def scrape_site(
        master_dir: Path,
        root: str = '/wps/wcm/connect',
        start_urls: Path = Path('start_urls.txt'),
        add_sitemap: bool = True,
        max_urls: int = 15_000,
        weekly: bool = True,
        monthly: bool = False,
        override: bool = False,
        basic: bool = False,
        trans: int = 0) -> None:
    """
    **Scrape www.belastingdienst.nl.**

    One complete scrape is done of www.belastingdienst.nl and stored in a
    timestamped sub-directory of master_dir. This so-named scrape directory
    will contain:

    - scrape database with: all scraped pages, all redirects that occurred
      while retrieving the pages, and detailed info per page
    - log file with info, warnings and/or errors of the scrape
    - copies of `robots.txt`, `sitemap.xml` and the `start_urls.txt` that were
      used to prime the scrape
    - scrape report if it is a `weekly` and/or `monthly` scrape

    When the `basic` parameter is `True`, the master database will be updated
    after the basic scrape has been performed with typical figures of the
    scrape. In case of a weekly or monthly scrape, the scrape is also added
    to the compiled history of the master database. But only if the scrape
    happens early in the week or month respectively, òr if date checking is
    overridden using the `override` parameter.

    The parameter `trans` determines whether the results of the scrape will
    be prepared for uu-based transmission. Depending on the actual value of
    `trans` the prepared uu-encoded txt-files will contain:

    -   trans 0: nothing
    -   trans 1: scrape database only
    -   trans 2: a zip file with the complete scrape directory
    -   trans 3 and higher: a zip with the complete scrape directory together
        with the master database
    -   in case of a basic scrape (determined by the `basic` parameter) the
        master database will not be included

    Arguments:

        master_dir: directory where the scrape results will go
        root: absolute path reference that sets the scope of the scrape
        start_urls: file with url's to start the scrape with
        add_sitemap: add url's from the sitemap to the start set
        max_urls: maximum number of url's that will be requested
        weekly: scrape is meant to be a weekly scrape
        monthly: scrape is meant to be a monthly scrape
        override: do not check dates on weekly and monthly scrapes
        basic: perform only a basic scrape
        trans: prepare for uu-based transmission

    Returns:

        None
    """

    # Setup various paths and create scrape directory
    timestamp = time.strftime('%y%m%d-%H%M')
    scrape_dir = master_dir / timestamp
    master_db = master_dir / 'scrape_master.db'
    scrape_db = scrape_dir / 'scrape.db'
    log_file = scrape_dir / 'log.txt'
    scrape_dir.mkdir()

    # Start logging
    logging.basicConfig(
        filename=str(log_file),
        format='[%(asctime)s] %(levelname)-8s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        force=True)
    start_time = time.time()
    logging.info(f'site scrape started')

    # Check dates
    warn_base = 'scrape is too far in <p> to be a valid <p>ly scrape'
    warn_over = ', but will continue since override is specified'
    warn_adhoc = ' and will continue as ad-hoc scrape'
    scrape_type = set()
    if monthly:
        # Is scrape within first days of the month?
        if time.localtime().tm_mday > _MAX_DOM:
            if override:
                logging.warning(warn_base.replace('<p>', 'month') + warn_over)
                scrape_type.add('monthly')
            else:
                logging.warning(warn_base.replace('<p>', 'month') + warn_adhoc)
                scrape_type.add('ad-hoc')
        else:
            scrape_type.add('monthly')
    if weekly:
        # Is scrape within first days of the week?
        if time.localtime().tm_wday + 1 > _MAX_DOW:
            if override:
                logging.warning(warn_base.replace('<p>', 'week') + warn_over)
                scrape_type.add('weekly')
            else:
                logging.warning(warn_base.replace('<p>', 'week') + warn_adhoc)
                scrape_type.add('ad-hoc')
        else:
            scrape_type.add('weekly')
    if not scrape_type:
        scrape_type.add('ad-hoc')
    scrape_type = '/'.join(scrape_type)

    # Setup scrape database
    root_url = _SITE + root
    create_scrape_db(scrape_db, root_url, timestamp, scrape_type)

    # Initialize some variables
    urls_todo = prime_scrape(start_urls, scrape_db, add_sitemap)
    urls_done = set()
    urls_scraped = 0
    pages_saved = 0

    while urls_todo and urls_scraped < max_urls:
        req_url = urls_todo.pop()
        wcm_url = scrape_page(
            req_url, root_url, urls_done, urls_todo, scrape_db)
        urls_scraped += 1
        if wcm_url:
            pages_saved += 1

        # Time cycles and print progress and prognosis
        num_todo = min(len(urls_todo), max_urls - urls_scraped)
        if urls_scraped % 25 == 0:
            page_time = (time.time() - start_time) / urls_scraped
            togo_time = int(num_todo * page_time)
            print(f'{urls_scraped:4} done, {page_time:.2f} sec per page / '
                  f'{num_todo:4} todo, {togo_time // 60}:{togo_time % 60:02} '
                  'min togo')

    elapsed = int(time.time() - start_time)
    logging.info(
        f'site scrape finished in {elapsed // 60}:{elapsed % 60:02} min')
    logging.info(f'total pages scraped: {pages_saved}')

    # Reduce db size by converting url's to id's in ed_links and redirs tables
    id_tables(scrape_db)

    # Extract and derive pages info
    extract_pages_info(scrape_db)
    derive_pages_info(scrape_db)

    if not basic:
        # Make id's universal
        sync_page_ids(scrape_db, master_db)

        # Write typical figures for this scrape to the master database
        key_figures(scrape_db, master_db)
        dimensions(scrape_db, master_db)

        if weekly or monthly:
            success = compile_history(
                scrape_db, master_db, check_dates=not override)
            if success:
                report_scrape(scrape_db, master_db, full_info=True)
        else:
            logging.info(f"scrape type is '{scrape_type}': "
                         "no history compiled and no report created")

    # Prepare databases for publication
    if trans == 1:
        uu_file = bintouu(scrape_db)
        split_uufile(uu_file, max_mb=30)
    elif trans > 1:
        zip_file = master_dir / (timestamp + '.zip')
        zf = zipfile.ZipFile(zip_file, mode='w')
        for f in scrape_dir.iterdir():
            zf.write(f, timestamp + '/' + f.name)
        if trans > 2 and not basic:
            zf.write(master_db, master_db.name)
        zf.close()
        uu_file = bintouu(zip_file)
        zip_file.unlink()
        split_uufile(uu_file, max_mb=30)

    # Conclude by sending an email with the log
    with open(log_file) as log:
        message = log.read()
    send_to = ['jos@diepnet.nl']
    mail_result(send_to, message)


def create_scrape_db(
        db_path: Path,
        root_url: str,
        timestamp: str,
        scrape_type: str) -> None:
    """
    **Create the scrape database.**

    Tables that are created:

    - `parameters`: characteristics of the scrape
    - `scr_wcm_paths`: binding wcm paths to a page_id
    - `pages`: source for all pages
    - `redirs`: redirects and wcm aliases that were encountered
    - `links`: all unique editorial links
    - `ed_links`: usage of links per page

    Arguments:

        db_path: path of the database file to create
        root_url: value to write to the 'root_url' parameter
        timestamp: value to write to the 'timestamp' parameter
        scrape_type: value to write to the 'scrape_type' parameter

    Returns:

        None
    """
    con = sqlite3.connect(db_path, isolation_level=None)
    con.execute('''
        CREATE TABLE parameters (
            name        TEXT PRIMARY KEY NOT NULL,
            value       TEXT NOT NULL)''')
    parameter(con, 'db_version', DB_VERSION)
    parameter(con, 'scraper_version', SCRAPER_VERSION)
    parameter(con, 'root_url', root_url)
    parameter(con, 'timestamp', timestamp)
    parameter(con, 'scrape_type', scrape_type)
    parameter(con, 'universal_page_ids', 'no')
    con.execute('''
        CREATE TABLE scr_wcm_paths (
            page_id	    INTEGER PRIMARY KEY AUTOINCREMENT,
            path        TEXT NOT NULL UNIQUE)''')
    con.execute('''
        CREATE TABLE pages (
            page_id     INTEGER PRIMARY KEY,
            doc         BLOB NOT NULL,
            FOREIGN KEY (page_id)
                REFERENCES scr_wcm_paths (page_id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT)''')
    con.execute('''
        CREATE TABLE redirs (
            req_id      INTEGER UNIQUE,
            req_url     TEXT UNIQUE,
            redir_id    INTEGER,
            redir_url   TEXT,
            redir_type  TEXT,
            FOREIGN KEY (req_id)
                REFERENCES scr_wcm_paths (page_id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT,
            FOREIGN KEY (redir_id)
                REFERENCES scr_wcm_paths (page_id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT)''')
    con.execute('''
        CREATE TABLE links (
            link_nr		INTEGER PRIMARY KEY AUTOINCREMENT,
            url			TEXT,
            anchor      TEXT,
            link_id		INTEGER,
            type		TEXT,
            status  	INTEGER,
            FOREIGN KEY (link_id)
                REFERENCES scr_wcm_paths (page_id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT)''')
    con.execute('''
        CREATE TABLE ed_links (
            page_id	    INTEGER NOT NULL,
            text 	    TEXT,
            link_nr	    INTEGER NOT NULL,
            FOREIGN KEY (page_id)
                REFERENCES scr_wcm_paths (page_id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT,
            FOREIGN KEY (link_nr)
                REFERENCES links (link_nr)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT)''')
    logging.info(f'scrape.db v{DB_VERSION} created')
    con.close()


def parameter(
        con: sqlite3.Connection,
        name: str,
        value: Union[str, int, float, bool] = None) -> Union[str, int, float]:
    """
    **Read or set a parameter value in a scrape database.**

    Arguments:

        con: connection to a scrape database
        name: parameter name
        value: parameter value; will be set when given, else only read

    Returns:

        parameter value
    """
    if value:
        con.execute('''
            INSERT OR REPLACE INTO parameters (name, value)
            VALUES (?, ?)''', [name, value])
    else:
        result = con.execute('''
            SELECT value
            FROM parameters
            WHERE name = ?''', [name]).fetchone()
        if not result:
            value = None
        else:
            value = result[0]
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass
    return value


def prime_scrape(start_urls: Path,
                 scrape_db: Path,
                 add_sitemap: bool = True) -> {str}:
    """
    **Set initial url's with which a scrape can be started.**

    A set of url's is returned that is a combination of the contents of the
    txt-file start_urls and those from sitemap.xml. This sitemap will be
    located via the sitemap-setting in robots.txt.
    All files that are used to prime the scrape will be copied to the scrape
    directory. Essential steps or failures will be logged.

    Arguments:

        start_urls: txt file with one full url per line
        scrape_db: scrape database
        add_sitemap: add all url's from sitemap.xml

    Returns:

        set of url's
    """

    # Build set of prime url's from the start_urls
    scrape_dir = scrape_db.parent
    prime_urls = set()
    shutil.copy2(start_urls, scrape_dir)
    with open(start_urls) as s:
        prime_urls |= {line.strip() for line in s}
    primers = [start_urls.name]

    if add_sitemap:
        # Find sitemap.xml via robots.txt and add url's from it to the set
        robots_url = _SITE + '/robots.txt'
        response = requests.get(robots_url)
        if response.status_code != 200:
            logging.info(
                f'getting {robots_url} returned status {response.status_code}')
        else:
            robots_txt = response.text
            with open(scrape_dir / 'robots.txt', 'w') as file:
                file.write(robots_txt)
            sitemap_urls = re.findall(r'Sitemap:\s(https?://\S*)', robots_txt)
            if len(sitemap_urls) == 0:
                logging.info('no sitemap.xml declared in robots.txt')
            else:
                for i, url in enumerate(sitemap_urls, start=1):
                    response = requests.get(url)
                    if response.status_code != 200:
                        logging.info(f'getting {url} returned status '
                                     f'{response.status_code}')
                    else:
                        logging.info(f'sitemap found at {url}')
                        if len(sitemap_urls) == 1:
                            filename = 'sitemap.xml'
                        else:
                            filename = f'sitemap({i}).xml'
                        primers.append(filename)
                        with open(scrape_dir / filename, 'w') as file:
                            file.write(response.text)
                        soup = BeautifulSoup(response.text, features='lxml')
                        for loc in soup.findAll('loc'):
                            prime_urls.add(loc.text)

    con = sqlite3.connect(scrape_db, isolation_level=None)
    parameter(con, 'primed_by', ', '.join(primers))
    con.close()

    return prime_urls


def scrape_page(
        req_url: str,
        root_url: str,
        urls_done: Set[str],
        urls_todo: Set[str],
        scrape_db: Path) -> Union[str, None]:
    """
    **Scrape an html page and save the results to the scrape database.**

    Get the final response from the requested url and save the page including
    the wcm path to the scrape database. Also add the redirections and
    editorial links to the scrape db. The urls_done and urls_todo sets are
    updated accordingly, driving the further scrape of the site.

    All url's are absolute, also in the resulting ed_links and redirs table.

    The returned wcm url is the content attribute of the `<meta
    name="DCTERMS.identifier">` tag. This is the url as generated by the WCM
    system.

    The returned value will be None when the requested url

    -   was already scraped (and saved)
    -   redirects to an out-of-scope page
    -   gave an unexpected response (which is logged)

    Arguments:

        req_url: url of requested page
        root_url: base url of the scrape; links starting with this url are
            interpreted as in scope for the scrape
        urls_done: url's that have already been scraped
        urls_todo: url's that still need to be scraped
        scrape_db: path to the scrape database

    Returns:

        wcm url (or None for the cases mentioned above)
    """

    def check_ext_link(url: str) -> (str, Union[int, str]):
        """
        **Return type and response status of an external link.**

        The returned status code is the one from the primary response upon
        requesting the given url. By default the type will be 'ext', but when
        the response is a redirect, depending on the destination url,
        '-to-int' or '-to-ext' is added to this type.

        When requesting the url hits an error, it is returned via the
        response status.

        Arguments:

            url: url to be checked

        Returns:

            (link type, response status)
        """
        l_type = 'ext'
        try:
            response = requests.get(
                url, allow_redirects=False, verify=False)
        except requests.exceptions.RequestException as err:
            resp_status = f'error while checking: {err}'
        else:
            resp_status = response.status_code
            if resp_status in (301, 302, 303, 307):
                location = response.headers['Location']
                if location.startswith(root_url):
                    l_type += '-to-int'
                else:
                    l_type += '-to-ext'
        return l_type, resp_status

    def save_page(path: str, doc: str) -> Union[int, None]:
        """
        **Save scraped page to the database.**

        Arguments:

            path: relative to the root_url of the scrape
            doc: page source

        Returns:

            page_id or None if already saved
        """
        p_id = con.execute(
            'SELECT page_id FROM scr_wcm_paths WHERE path = ?',
            [path]).fetchone()
        if p_id:
            # Page was already saved during this scrape
            return None
        else:
            # Page was not saved yet during this scrape
            p_id = con.execute(
                'INSERT INTO scr_wcm_paths (path) VALUES (?)',
                [path]).lastrowid
            con.execute(
                'INSERT INTO pages (page_id, doc) VALUES (?, ?)',
                [p_id, zlib.compress(doc.encode())])
            return p_id

    # Setup some variables
    page_as_string = ''
    soup = None
    redir_qry = '''
        INSERT OR IGNORE INTO redirs (req_url, redir_url, redir_type) 
        VALUES (?, ?, ?)'''
    con = sqlite3.connect(scrape_db, isolation_level=None)

    # Request loop
    while True:
        # Keep requesting until no further rewrites or redirects occur,
        # while saving redirects and updating the urls_done admin.
        resp = requests.get(req_url, allow_redirects=False)
        urls_done.add(req_url)

        # Check and handle status codes
        if resp.status_code == 200:
            pass
        elif resp.status_code in (301, 302):
            # This concerns a redirection response
            redir_url = resp.headers['Location']
            # If request or redir is in scope, save the redirect with full url's
            if req_url.startswith(root_url) or redir_url.startswith(root_url):
                con.execute(redir_qry, [req_url, redir_url, resp.status_code])
            if redir_url.startswith(root_url):
                # restart Request loop with redirected url
                req_url = redir_url
                continue
            else:
                # Redirect url is out of scope.
                return None
        else:
            # Returned status code is not 200, 301 or 302, so getting the page
            # did not succeed. Consider it done and return a None result.
            logging.error(f'unexpected response from {req_url}; '
                          f'status code is {resp.status_code}.')
            return None

        # Read and parse the response into a soup document
        page_as_string = resp.text
        soup = BeautifulSoup(page_as_string, features='lxml')

        # Has this page a client-side redirect by means of header tag
        # <meta http-equiv="refresh" content="0;url=...">?
        meta_tag = soup.find('meta', attrs={'http-equiv': 'refresh'})
        if meta_tag:
            redir_url = meta_tag['content'].split('url=')[1]
            redir_url = urljoin(req_url, redir_url)
            # If request or redir is in scope, save the redirect with full url's
            if req_url.startswith(root_url) or redir_url.startswith(root_url):
                con.execute(redir_qry, [req_url, redir_url, 'client'])
            if redir_url.startswith(root_url):
                # Restart Request loop with redirected url
                req_url = redir_url
                continue
            else:
                # Redirect url is out of scope.
                return None

        # End of the Request loop.
        # Reaching this point means we're done with this page.
        break

    # Determine the wcm url
    if resp.url.startswith(root_url):
        # Response url is in scope. Now the wcm url should be in the header
        # meta tag named 'DCTERMS.identifier'.
        meta_tag = soup.find(
            'meta', attrs={'name': 'DCTERMS.identifier'})
        if meta_tag:
            wcm_url = meta_tag['content']
            wcm_url = urljoin(resp.url, wcm_url)
            if wcm_url != resp.url:
                # Save this as an alias in the redirects table
                con.execute(redir_qry, [resp.url, wcm_url, 'alias'])
                # Consider also the wcm_url as done
                urls_done.add(wcm_url)
        else:
            logging.error(f'Page without wcm url; '
                          f'falling back to: {resp.url}')
            wcm_url = resp.url
    else:
        # Response url is not in scope.
        # Because of the logic from the code above this situation should
        # never occur. Since this is not destructive, scraping continues.
        logging.error(f'out of scope response from {req_url}')
        return None

    # Save in-scope page under the wcm path.
    # It is possible that a page in the urls_todo set redirects to a wcm_url
    # of which the page/path combination is saved already. Especially some
    # short url's cause this situation. Those cases are not saved (again).
    wcm_path = wcm_url.split(root_url)[1]
    page_id = save_page(wcm_path, page_as_string)
    if not page_id:
        # Page was saved and handled earlier during this scrape
        return None

    # Fetch all in-page links, save the editorial links to the scrape db and
    # add links of in-scope pages to the urls_todo set.
    # Cycle over the editorial, automated en rest (partial) trees of the page
    for tree in split_tree(soup):
        editorial = True if tree.html['tree'] == 'editorial' else False

        # Cycle over all links in the tree
        for a_tag in tree.find_all('a', href=True):
            link = a_tag['href'].strip()
            # Some links are discarded
            if link in ('#', '/'):
                continue
            if 'readspeaker' in link or 'adobe' in link or 'java' in link:
                continue
            link = urljoin(resp.url, link)
            # Separate anchor from in-scope url
            if link.startswith(root_url) and '#' in link:
                link, _, anchor = link.partition('#')
            else:
                anchor = None
            if link == wcm_url:
                # Discard in-page link
                continue
            if link and editorial:
                # If this link was not saved already, add it to the links
                # table, including the type. Add status in case it is external
                # relative to the root_url.
                qry_result = con.execute(
                    'SELECT link_nr FROM links WHERE url = ? AND anchor IS ?',
                    [link, anchor]).fetchone()
                if qry_result:
                    link_nr = qry_result[0]
                else:
                    link_type = None
                    link_status = None
                    if re.match('^https?://', link):
                        if link.startswith(root_url):
                            link_type = 'int'
                        else:
                            link_type, link_status = check_ext_link(
                                link + '#' + anchor if anchor else link)
                            # The link is a short url if it redirects to an
                            # internal url. In that case it should be scraped.
                            if link_status == 'ext-to-int':
                                if link not in urls_done:
                                    urls_todo.add(link)
                    link_nr = con.execute('''
                        INSERT INTO links (url, anchor, type, status)
                        VALUES (?, ?, ?, ?)''', [link, anchor, link_type,
                                                 link_status]).lastrowid
                # Save link in relation to the page
                link_text = a_tag.text.strip()
                con.execute('''
                    INSERT INTO ed_links (page_id, text, link_nr)
                    VALUES (?, ?, ?)''',
                            [page_id, link_text, link_nr])
            if link:
                # Discard links that we do not want to scrape
                if link.endswith('.xml'):
                    continue
                if not link.startswith(root_url):
                    # Link not in scope
                    continue
                if link in urls_done:
                    # Already handled
                    continue
                urls_todo.add(link)

    return wcm_url


def split_tree(
        soup: BeautifulSoup) -> (BeautifulSoup, BeautifulSoup, BeautifulSoup):
    """
    **Split a soup doc in three separate docs: editorial, automated and rest.**

    The editorial doc is a copy of the originating doc with all tags removed
    that do not contain editorial content. The automated doc is a new html (
    root) tag containing all tags with automated content. The rest doc is an
    html (root) tag containing all tags that did not represent editorial or
    automated content. No tags are lost, so all three docs together contain
    the same tags as the originating doc. The hierarchical structure of the
    trees is lost however.

    To identify the separate docs, a `tree` attribute is added to the `<html>`
    tag with values `editorial`, `automated` and `rest` respectively.

    More specifically, the three docs will contain the next tags from the
    originating doc.

    rest doc:

    - `<head>`
    - `<header>`
    - `<footer>`
    - `<div id="bld-nojs">`: for situation that javascript is not active
    - `<div class="bld-subnavigatie">`: left side navigation of bib pages
    - `<div class="bld-feedback">`: bottom feedback of content page
    - `<div>` with readspeaker buttons
    - `<div>` with modal dialog for the virtual assistant

    automated doc:

    - `<div class="add_content">`: automatically added content
    - all remaining tags from pages with type bld-overview

    editorial doc:

    - all tags that do not go to one of the other docs

    Arguments:

        soup: bs4 representation of the page to be split

    Returns:

        (editorial doc, automated doc, rest doc)
    """

    # Make working copy from soup doc, because of destructive tag removal
    tree = copy.copy(soup)
    # Create rest doc as html trunk
    rst_tree = BeautifulSoup('<html tree="rest"></html>', features='lxml')

    # All that is not needed for the editorial or automated trees goes to rest:
    # - head, header and footer
    if tree.head:
        rst_tree.html.append(tree.head.extract())
    if tree.body.header:
        rst_tree.html.append(tree.body.header.extract())
    if tree.body.footer:
        rst_tree.html.append(tree.body.footer.extract())
    # - content for non active javascript
    div_nojs = tree.find('div', id='bld-nojs')
    if div_nojs:
        rst_tree.html.append(div_nojs.extract())
    # - sub-navigation
    div_subnav = tree.find(class_='bld-subnavigatie')
    if div_subnav:
        rst_tree.html.append(div_subnav.extract())
    # - feedback
    div_feedback = tree.find(class_='bld-feedback')
    if div_feedback:
        rst_tree.html.append(div_feedback.extract())
    # - readspeaker buttons
    for tag in tree.find_all('div', class_='rs_skip'):
        rst_tree.html.append(tag.extract())
    # - modal dialog for the virtual assistant
    for tag in tree.find_all('div', id='vaModal'):
        rst_tree.html.append(tag.extract())

    # Is the page generated without any editor intervention?
    if tree('body', attrs={'data-pagetype': 'bld-overview'}):
        # Then editor tree becomes trunk only
        ed_tree = BeautifulSoup(
            '<html tree="editorial"></html>', features='lxml')
        # And automated tree gets all remaining content
        aut_tree = tree
        aut_tree.html['tree'] = 'automated'
    else:
        # Make new html trunk and graft all automated content on it
        aut_tree = BeautifulSoup(
            '<html tree="automated"></html>', features='lxml')
        for tag in tree.find_all('div', class_='content_add'):
            aut_tree.html.append(tag.extract())
        # Editorial tree gets all remaining content
        ed_tree = tree
        ed_tree.html['tree'] = 'editorial'

    return ed_tree, aut_tree, rst_tree


def extract_pages_info(scrape_db: Path) -> None:
    """
    **Create pages_info table with information extracted from all pages.**

    Extracted information concerns data that is readily available within
    the page that is stored in the pages table. Storing this data in a
    separate table is strictly redundant, but serves faster access.

    The fields of the pages_info table are defined by the _EXTRACTED_FIELDS
    and _DERIVED_FIELDS constants. The contents of these fields is documented
    with these constants.

    An existing pages_info table is removed before creating a new one.

    The pages_info table accommodates additional fields to contain
    derived information for each page. This is further detailed in the
    derive_pages_info function.

    It will be logged when tags or attributes are missing or values are
    invalid.

    Arguments:

        scrape_db: path to the scrape database

    Returns:

        None
    """

    con = sqlite3.connect(scrape_db, isolation_level=None)

    # Create new pages_info table.
    # This table is not included when the scrape database is created. While
    # adding it separately it is possible to change and recreate this table
    # to refill it afterwards.
    con.execute('DROP TABLE IF EXISTS pages_info')
    fields = _EXTRACTED_FIELDS + _DERIVED_FIELDS
    info_columns = ', '.join([f'{f[0]} {f[1]}' for f in fields])
    con.execute(f'''
        CREATE TABLE pages_info (
            page_id INTEGER PRIMARY KEY,
            {info_columns},
            FOREIGN KEY (page_id)
            REFERENCES scr_wcm_paths (page_id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT)''')
    logging.info('new pages_info table created in scrape.db')

    num_pages = con.execute('SELECT count(*) FROM pages').fetchone()[0]
    start_time = time.time()

    # Cycle over all pages
    qry = '''
        SELECT page_id, path, doc
        FROM scr_wcm_paths
        LEFT JOIN pages USING (page_id)'''
    for page_num, (page_id, path, doc) in enumerate(con.execute(qry), start=1):
        page_string = zlib.decompress(doc).decode()
        soup = BeautifulSoup(page_string, features='lxml')
        info = {'page_id': page_id}

        # Get title
        title = soup.title
        if not title:
            logging.warning(f'page has no <title> tag: {path}')
            title = None
        else:
            title = title.text
            if not title:
                logging.warning(f'page with empty title: {path}')
        info['title'] = title

        # Get description
        description = soup.find(attrs={'name': 'description'})
        if not description:
            # There are very much occurrences of this situation
            logging.debug(
                f'page has no <meta name="description"/> tag: {path}')
            description = None
        else:
            description = description['content']
            if not description:
                logging.warning(f'page with empty description: {path}')
        info['description'] = description

        # Get info from <h1> tags
        h1s = []
        for h1 in soup.find_all('h1'):
            h1s.append(h1.text)
        if len(h1s) == 0:
            logging.warning(f'page without h1: {path}')
        info['num_h1s'] = len(h1s)
        info['first_h1'] = h1s[0] if h1s else None

        # Get language
        language = soup.find('meta', attrs={'name': 'language'})
        if not language:
            logging.warning(
                f'page has no <meta name="language"/> tag: {path}')
            language = None
        else:
            language = language['content']
            if not language:
                logging.warning(f'page with empty language: {path}')
        info['language'] = language

        # Get date modified
        modified = soup.find('meta', attrs={'name': 'DCTERMS.modified'})
        if not modified:
            logging.warning(
                f'page has no tag <meta name="DCTERMS.modified"/>: {path}')
            modified = None
        else:
            try:
                modified = date.fromisoformat(modified['content'])
            except ValueError:
                logging.warning(
                    f'page with improper modification date: {path}')
                modified = None
        info['modified'] = modified

        # Get type of page
        if 'data-pagetype' not in soup.body.attrs:
            logging.warning('page has no data-pagetype attribute in the '
                            f'<body> tag: {path}')
            pagetype = None
        else:
            pagetype = soup.body['data-pagetype']
            if not pagetype:
                logging.warning(
                    f'page with empty pagetype in <body> tag: {path}')
        info['pagetype'] = pagetype

        # Get classes
        if 'class' not in soup.body.attrs:
            logging.warning(
                f'page has no class attribute in the <body> tag: {path}')
            classes = None
        else:
            classes = soup.body['class']
            if not classes:
                logging.warning(
                    f'page with empty class in <body> tag: {path}')
        info['classes'] = ' '.join(classes) if classes else None

        # Get editorial and automated texts
        info['ed_text'], info['aut_text'] = get_text(soup)

        # add info to the database
        fields = ', '.join(info)
        q_marks = ('?, ' * len(info))[:-2]
        con.execute(f'INSERT INTO pages_info ({fields}) VALUES ({q_marks})',
                    list(info.values()))

        # print progress and prognosis
        if page_num % 250 == 0:
            page_time = (time.time() - start_time) / page_num
            togo_time = int((num_pages - page_num) * page_time)
            print(
                f'adding extracted info to scrape database '
                f'- togo: {num_pages - page_num} pages / '
                f'{togo_time // 60}:{togo_time % 60:02} min')

    con.close()
    logging.info('info extracted from all pages')


def get_text(soup: BeautifulSoup) -> [str, str]:
    """
    **Retrieve essential editorial and automated text content of a page.**

    The editorial and automated text of the page content is returned together
    as a twofold tuple. Basically the relevant texts are retrieved from
    partial trees containing only tags with editorial or automated content
    respectively. Whitespace within these texts is normalised and coherent
    chunks are separated by newlines.

    Arguments:

        soup: bs4 representation of a page

    Returns:

        [editorial text, automated text] of the page
    """

    result = []
    for tree in split_tree(soup):
        if tree.html['tree'] == 'rest':
            continue

        flatten_tagbranch_to_navstring(tree.html)

        # Replace non-breaking spaces with normal ones
        txt = tree.text.replace(b'\xc2\xa0'.decode(), ' ')

        # Substitute one space for any cluster of whitespace chars (getting rid
        # of returns, newlines, tabs, spaces, etc.; this is html, you know!).
        txt = re.sub(r'\s+', ' ', txt)

        # Change #br# markers (introduced while flattening the branches) to
        # newlines, while reducing multiples separated by whitespace only.
        # The final strip() removes potential trailing newlines.
        txt = re.sub(r'\s*(#br#\s*)+\s*', r'\n', txt).strip()

        result.append(txt)
        tree.decompose()

    return result


def flatten_tagbranch_to_navstring(tag: Tag) -> None:
    """
    **Reduce a complete tag branch to one NavigableString.**

    The reduction is realised within the BeautifulSoup data structure that
    the tag is part of. This means that the function replaces the tag branch
    (in place) into a single NavigableString containing all text of the
    complete tag branch.

    The function uses a recursive tree traversal algorithm with a
    NavigableString as leaf. Each instance of the function will combine the
    text content of all children into one NavigableString. Within this string
    all `<br>` tags are replaced by `#br#` markers. The text content of all
    former `<p>`, `<h1>`, `<h2>`, `<h3>`, `<li>` and `<div>` tags in the tag
    branch is enclosed between two `#br#` markers in the resulting
    NavigableString. As such `#br#` markers act as separators between logical
    chunks of text. Due to the recursive flattening process the resulting
    NavigableString may contain more consecutive `#br#` markers. Since
    lay-out is of no concern, this has no significance however.

    Arguments:

        tag: part of BeautifulSoup structure that will be reduced

    Returns:

        None (tag is replaced in place with one NavigableString)
    """

    # Final leaf cases; done with this branch
    if type(tag) in {NavigableString, Comment, Script, Stylesheet}:
        return

    # Has this tag children other then NavigableStrings?
    tag_children = list(tag.children)
    child_types = {type(c) for c in tag_children}
    if tag_children and child_types != {NavigableString}:
        # Flatten recursively all child branches to NavigableStrings
        for c in tag_children:
            flatten_tagbranch_to_navstring(c)

    # At this point all children (if any) of tag are NavigableStrings
    tag_name = tag.name
    if tag_name == 'br':
        tag.replace_with('#br#')
    elif tag_name == 'a':
        tag.replace_with(f' {tag.text}')  # The leading space is significant
    elif tag_name in {'p', 'h1', 'h2', 'h3', 'li', 'div'}:
        tag_text = tag.text
        tag.replace_with(f'#br#{tag_text}#br#')
    else:
        tag.replace_with(tag.text)

    return


def derive_pages_info(scrape_db: Path) -> None:
    """
    **Add derived information for all pages.**

    Derived information as such is not available within a page,
    but calculated or interpreted from other information. To derive this
    information, the extracted information should already be available in the
    pages_info table. This can be accomplished by using the
    extract_pages_info function.

    The fields in which the derived info is saved, are already available in
    the pages_info table. In case a field is added, the extract_pages_info
    method can be used to recreate the pages_table, implicitly adding the
    extra fields.

    The constants _EXTRACTED_FIELDS and _DERIVED_FIELDS together define the
    fields that are created in the pages_info table. The contents of these
    fields is documented with these constants.

    It will be logged when info can not be derived due to inconsistent or
    unavailable information.

    Arguments:

        scrape_db: path to the scrape database

    Returns:

        None
    """

    # Connect the database and clear the derived field contents
    con = sqlite3.connect(scrape_db, isolation_level=None)
    set_cols = ', '.join([f'{f[0]} = NULL' for f in _DERIVED_FIELDS])
    con.execute(f'UPDATE pages_info SET {set_cols}')

    # Setup some queries
    bus_qry = 'UPDATE pages_info SET business = ? WHERE page_id = ?'
    cat_qry = 'UPDATE pages_info SET category = ? WHERE page_id = ?'
    for_qry = '''
        SELECT page_id, pagetype, classes
        FROM pages_info
        ORDER BY CASE pagetype WHEN 'bld-wrapper' THEN 2 ELSE 1 END'''
    cat_groups_qry = '''
        SELECT category
        FROM ed_links
        LEFT JOIN links USING  (link_nr)
        LEFT JOIN pages_info USING (page_id)
        WHERE link_id = ?
        GROUP BY category'''
    wrappers_without_cat = set()

    # Cycle over all pages, with wrapper pages after all others, because
    # the wrapper category is determined by the categories of pages
    # linking to that wrapper page.
    for page_id, pagetype, classes in con.execute(for_qry):

        # Determine business
        if classes:
            if 'toeslagen' in classes:
                business = 'toeslagen'
            elif 'douane' in classes:
                business = 'douane'
            else:
                business = 'belastingen'
        else:
            business = None
        con.execute(bus_qry, [business, page_id])

        # Determine category: dv, bib or alg
        if pagetype in _DV_TYPES:
            category = 'dv'
        elif pagetype in _BIB_TYPES:
            category = 'bib'
        elif pagetype in _ALG_TYPES:
            category = 'alg'
        elif pagetype == 'bld-wrapper':
            # Group the categories of all pages that link to this wrapper page
            categories = con.execute(cat_groups_qry, [page_id]).fetchall()
            if len(categories) == 1:
                # All pages linking to this wrapper have the same category
                category = categories[0][0]
                if not category:
                    # Probably because other uncategorized wrappers are linking
                    # to this one. Save its page_id for post-processing.
                    wrappers_without_cat.add(page_id)
            else:
                category = 'alg'
        else:
            category = 'unknown'
        con.execute(cat_qry, [category, page_id])

    # The main reason that wrappers did not get a category in the main loop,
    # is that the pages that link to these wrappers are wrappers themselves
    # that had no category yet when asked for in the main loop. Repeating the
    # loop for the category of these pages will resolve most of them. To
    # avoid endless looping when wrappers without categories are linking to
    # each other, the number of cycles is maximized (crude but effective).
    max_cycles = len(wrappers_without_cat) * 3
    cycle = 0
    while wrappers_without_cat and cycle < max_cycles:
        cycle += 1
        page_id = wrappers_without_cat.pop()
        # Algorithm for category resolution is the same as in the main loop
        categories = con.execute(cat_groups_qry, [page_id]).fetchall()
        if len(categories) == 1:
            category = categories[0][0]
            if not category:
                wrappers_without_cat.add(page_id)
                continue
        else:
            category = 'alg'
        con.execute(cat_qry, [category, page_id])

    # Log the remaining cases and set their category to 'alg' to avoid NULLs
    # in the database.
    for page_id in wrappers_without_cat:
        path = con.execute(f'SELECT path FROM scr_wcm_paths '
                           f'WHERE page_id = {page_id}').fetchone()[0]
        logging.warning(f"unresolved category of wrapper set to 'alg': {path}")
        con.execute(cat_qry, ['alg', page_id])

    con.close()
    logging.info('info derived from all pages')


def id_tables(scrape_db: Path) -> None:
    """
    **Convert link url's to page_id's in the links and redirs tables.**

    After a scrape has finished, the url's in the links and redirs tables
    contain absolute url's. Converting these url's to page_id's where
    possible reduces the size of the database considerably. Due to the
    relative sizes, this is more relevant for the links than for the redirs
    table.

    Because of the size reduction that this function realizes, the function
    vacuums the database before returning.

    Arguments:

        scrape_db: path to the scrape database

    Returns:

        None
    """

    con = sqlite3.connect(scrape_db, isolation_level=None)
    root_url = parameter(con, 'root_url')

    # Reduce in-scope url's to paths
    con.executescript(f'''
        UPDATE links
        SET url = replace(url, '{root_url}', '')
        WHERE url LIKE '{root_url}%' ''')

    # Get id's for the links
    con.executescript(f'''
        UPDATE links
        SET link_id = (
            SELECT page_id
            FROM scr_wcm_paths
            WHERE url = path)''')
        
    # Remove url's where id's are available
    con.executescript(f'''
        UPDATE links
        SET url = NULL
        WHERE link_id IS NOT NULL''')
        
    # Reform paths that got no id to full url's again (including potential
    # anchors).
    con.executescript(f'''
        UPDATE links
        SET url = '{root_url}' || url 
                || CASE WHEN anchor IS NULL THEN '' ELSE '#' || anchor END,
            anchor = NULL
        WHERE url LIKE '/%' ''')

    # Convert req_url's to id's where possible while removing the url
    con.execute(f'''
        UPDATE redirs
        SET req_id = (
                SELECT page_id
                FROM scr_wcm_paths
                WHERE req_url = '{root_url}' || path),
            req_url = NULL
        WHERE req_url IN
            (SELECT '{root_url}' || path
            FROM scr_wcm_paths)''')

    # Convert redir_url's to id's where possible while removing the url
    con.execute(f'''
        UPDATE redirs
        SET redir_id = (
                SELECT page_id
                FROM scr_wcm_paths
                WHERE redir_url = '{root_url}' || path),
            redir_url = NULL
        WHERE redir_url IN
            (SELECT '{root_url}' || path
            FROM scr_wcm_paths)''')

    # Add view with each internal request url from the redirs table redirecting
    # directly to the final cms url. The view is not needed currently, but still
    # created because of the complex sql query.
    con.execute('DROP VIEW IF EXISTS redirs_to_cms')
    con.execute('''
        CREATE VIEW redirs_to_cms (req_url, cms_url) AS
        WITH
            -- build url-based redir chains
            chains (org_url, prev_url, next_id, next_url, next_type) AS (
                SELECT req_url, NULL, redir_id, redir_url, redir_type
                FROM redirs
                    UNION ALL
                SELECT org_url, next_url, r.redir_id, r.redir_url, r.redir_type
                FROM chains AS c
                LEFT JOIN redirs AS r ON c.next_url = r.req_url
                -- avoid endless loops by excluding potential redirs of an alias
                WHERE c.next_type <> 'alias'  
            ),
            -- get all redir chains ending in an alias
            chains_to_alias (req_url, cms_id) AS (
                SELECT org_url, next_id
                FROM chains
                WHERE next_type = 'alias'
                    AND org_url IS NOT NULL
            )
        -- combining all types of redir chains to a cms url
        SELECT
            req_url,
            CASE
                WHEN c.cms_id IS NULL 
                THEN r.redir_id 
                ELSE c.cms_id 
            END AS cms_id
        FROM redirs AS r
        LEFT JOIN chains_to_alias AS c USING (req_url)
        WHERE req_url IS NOT NULL
        ORDER BY req_url''')

    con.execute('VACUUM')
    con.close()
    logging.info("url's referring to wcm pages in links and redirs tables "
                 "converted to id's")


def sync_page_ids(scrape_db, master_db):
    """
    **Synchronise page id's and paths of a scrape with the master database.**

    New paths of the scrape are added to the mst_wcm_paths table of the
    master database. Subsequently all page id's within the scrape database
    are synchronised against the id's of this mst_wcm_paths table. This makes
    page id's universal over all scrapes.

    **Coding note:**

    *It has been tried to update the pages_id's in the `scr_wcm_paths` table
    only, leaving updating of the related tables to sqlite's foreign
    constraint cascading mechanism. Regrettably that brought sqlite to a
    grinding halt. Instead of this method an algorithm is used via which all
    table columns with a name ending in `_id` are updated with the id's from
    the `mst_wcm_paths` table in the master database. When this update is
    interrupted the relations within the scrape database become corrupted
    beyond repair. So all queries implementing this total update have been
    wrapped in a `BEGIN/COMMIT TRANSACTION` pair.*

    Arguments:

        scrape_db (Path): path to the scrape database
        master_db (Path): path to the master database

    Returns:

        None
    """

    con = sqlite3.connect(scrape_db, isolation_level=None)
    con.execute(f'ATTACH "{master_db}" AS master')

    # Add new paths from scrape to the master paths table
    con.execute('''
        WITH
            new_scr_pages AS (
                SELECT path
                FROM scr_wcm_paths AS scr
                LEFT JOIN mst_wcm_paths AS mst USING (path)
                WHERE mst.page_id IS NULL
                ORDER BY path
            )
        INSERT INTO mst_wcm_paths (path)
        SELECT path
        FROM new_scr_pages''')

    # Create a temporary table to change the scrape page_id's with negated old
    # page_id's (to avoid unique key constraint failures; see also further).
    con.execute('''
        CREATE TEMP TABLE id_trans AS
        SELECT -scr.page_id AS old_id, mst.page_id AS new_id
        FROM scr_wcm_paths AS scr
        LEFT JOIN mst_wcm_paths AS mst USING (path)''')

    # Although set by default, we want to be sure that foreign key constraints
    # are disabled.
    con.execute('PRAGMA foreign_keys = FALSE')

    # It is imperative that all id updates finish successfully, so we use BEGIN
    # and COMMIT TRANSACTION around the queries.
    con.execute('BEGIN TRANSACTION')

    # Query to get all table/column combinations where column name ends on '_id'
    qry = '''
        SELECT m.name AS table_name , p.name AS column_name
        FROM sqlite_master AS m
        JOIN pragma_table_info(m.name) AS p
        WHERE column_name LIKE '%_id' '''

    # Cycle over all columns to be updated
    for table, column in con.execute(qry).fetchall():

        # To avoid unique constraint failures first negate id's.
        qry = f'UPDATE {table} SET {column} = -{column}'
        con.execute(qry)

        # Update the id's now for this column
        qry = f'''
            UPDATE {table}
            SET {column} = (
                SELECT new_id
                FROM id_trans
                WHERE {column} = old_id)'''
        con.execute(qry)

    con.execute('COMMIT TRANSACTION')
    parameter(con, 'universal_page_ids', 'yes')
    con.execute('VACUUM')
    con.close()
    logging.info("page id's synchronised with master database")


def key_figures(scrape_db: Path, master_db: Path) -> None:
    """
    **Write typical figures from a scrape to the master database.**

    Potential previous figures from the scrape will be deleted before adding
    the new set.

    Next number of pages, redirs, url-aliases or ed_links will be written to
    the key_figures table of the master database:

    - pages: all pages
    - pages_lang_`<language>`: per `<language>`
    - pages_buss_`<business>`: per `<business>`
    - pages_cat_`<category>`: per `<category>`
    - pages_type_`<pagetype>`: per `<pagetype>`
    - pages_h1_multi: with more than one h1-tag
    - pages_h1_multi_`<pagetype>`: with more than one h1-tag per `<pagetype>`
    - pages_h1_no: without h1-tag
    - pages_title_no: without or with empty title-tag
    - pages_title_dupl: with non-unique title-tag
    - pages_descr_no: without or with empty description meta-tag
    - pages_descr_long: with description meta-tag longer than 160 characters
    - redirs: total number of all redirects
    - redirs_`<type>`: number of redirects per type
    - redirs_`<type>`_slash: redirects per type with only differing a slash
    - redirs_wcm-url: wcm url's that get redirected
    - url-aliases: url's that alias an wcm url
    - url-aliases_`<num>`x: url's with `<num>` aliases
    - ed-links_`<int|ext>`: internal|external editorial links
    - ed-links_`<int|ext>`_uniq: unique internal|external editorial links
    - ed-links_`<int|ext>`_avg: internal|external editorial links per page
    - ed-links_int_redir: redirected internal editorial links
    - ed-links_int_non-wcm: internal editorial links to non-wcm url's

    Arguments:

        scrape_db: path to the scrape database
        master_db: path to the master database

    Returns:

        None
    """

    # Connect databases and get some parameter values
    con = sqlite3.connect(master_db, isolation_level=None)
    con.execute(f'ATTACH "{scrape_db}" AS scrape')
    timestamp = parameter(con, 'timestamp')
    con.execute(f"DELETE FROM key_figures WHERE timestamp = '{timestamp}'")

    ins_qry = f'''
        INSERT INTO key_figures (timestamp, name, value)
        VALUES ('{timestamp}', ?, ?)'''

    # Total pages
    qry = '''
        SELECT count(*)
        FROM pages_info'''
    num_pages = con.execute(qry).fetchone()[0]
    con.execute(ins_qry, ['pages', num_pages])

    # Pages per language
    qry = '''
        SELECT language, count(*)
        FROM pages_info 
        GROUP BY language
        ORDER BY language DESC'''
    for language, count in con.execute(qry).fetchall():
        con.execute(ins_qry, [f'pages_lang_{language}', count])

    # Pages per business
    qry = '''
        SELECT business, count(*)
        FROM pages_info 
        GROUP BY business
        ORDER BY business'''
    for business, count in con.execute(qry).fetchall():
        con.execute(ins_qry, [f'pages_buss_{business}', count])

    # Pages per category
    qry = '''
        SELECT category, count(*)
        FROM pages_info 
        GROUP BY category
        ORDER BY business DESC'''
    for category, count in con.execute(qry).fetchall():
        con.execute(ins_qry, [f'pages_cat_{category}', count])

    # Pages per type
    qry = '''
        SELECT pagetype, count(*)
        FROM pages_info
        GROUP BY pagetype
        ORDER BY category DESC, count(*) ASC'''
    for pagetype, count in con.execute(qry).fetchall():
        con.execute(ins_qry, [f'pages_type_{pagetype}', count])

    # Pages with more than one h1's
    qry = '''
        SELECT count(*)
        FROM pages_info
        WHERE num_h1s > 1'''
    con.execute(ins_qry, ['pages_h1_multi', con.execute(qry).fetchone()[0]])

    # Pages per type with more than one h1's
    qry = '''
        SELECT pagetype, count(*)
        FROM pages_info
        WHERE num_h1s > 1
        GROUP BY pagetype'''
    for pagetype, count in con.execute(qry).fetchall():
        con.execute(ins_qry, [f'pages_h1_multi_{pagetype}', count])

    # Pages with no h1
    qry = '''
        SELECT count(*) 
        FROM pages_info 
        WHERE num_h1s = 0'''
    con.execute(ins_qry, ['pages_h1_no', con.execute(qry).fetchone()[0]])

    # Pages without title
    qry = '''
        SELECT count(*) 
        FROM pages_info 
        WHERE title = '' or title is NULL'''
    con.execute(ins_qry, ['pages_title_no', con.execute(qry).fetchone()[0]])

    # Pages with non unique title
    qry = '''
        WITH
            title_freq AS (
                SELECT count(*) as c
                FROM pages_info
                GROUP BY title
            )
        SELECT CASE WHEN sum(c) IS NULL THEN 0 ELSE sum(c) END
        FROM title_freq
        WHERE c > 1'''
    con.execute(ins_qry, ['pages_title_dupl', con.execute(qry).fetchone()[0]])

    # Pages without description
    qry = '''
        SELECT count(*) 
        FROM pages_info 
        WHERE description = '' or description is NULL'''
    con.execute(ins_qry, ['pages_descr_no', con.execute(qry).fetchone()[0]])

    # Pages with description longer than 160 characters
    qry = '''
        SELECT count(*) 
        FROM pages_info 
        WHERE length(description) > 160'''
    con.execute(ins_qry, ['pages_descr_long', con.execute(qry).fetchone()[0]])

    # Total redirects (an alias is no redirect)
    qry = '''
        SELECT count(*)
        FROM redirs
        WHERE redir_type != "alias"'''
    con.execute(ins_qry, ('redirs', con.execute(qry).fetchone()[0]))

    # Redirects per type
    qry = '''
        SELECT redir_type, count(*)
        FROM redirs
        WHERE redir_type != 'alias'
        GROUP BY redir_type'''
    for redir_type, count in con.execute(qry).fetchall():
        con.execute(ins_qry, (f'redirs_{redir_type}', count))

    # Create temporary view for some key figures that follow below
    qry = '''
        CREATE TEMP VIEW redirs_full_urls (req_url, redir_url, redir_type) AS
        SELECT
            CASE
                WHEN req_url IS NULL
                THEN root_url || p1.path
                ELSE req_url
            END,
            CASE
                WHEN redir_url IS NULL
                THEN root_url || p2.path 
                ELSE redir_url
            END,
            redir_type
        FROM redirs
        JOIN (SELECT value AS root_url FROM parameters WHERE name = 'root_url')
        LEFT JOIN scr_wcm_paths AS p1 ON req_id = p1.page_id
        LEFT JOIN scr_wcm_paths AS p2 ON redir_id = p2.page_id'''
    con.execute(qry)

    # Redirects per type that only add or loose the last slash
    qry = '''
        SELECT redir_type, count(*)
        FROM redirs_full_urls
        WHERE req_url || '/' = redir_url or req_url = redir_url || '/'
        GROUP BY redir_type'''
    for redir_type, count in con.execute(qry).fetchall():
        con.execute(ins_qry, (f'redirs_{redir_type}_slash', count))

    # Wcm url's that are redirected
    qry = '''
        SELECT count(*)
        FROM redirs
        WHERE req_id IS NOT NULL'''
    con.execute(ins_qry, ['redirs_wcm-url', con.execute(qry).fetchone()[0]])

    # Total aliases
    qry = '''
        SELECT count(*)
        FROM redirs
        WHERE redir_type = "alias"'''
    con.execute(ins_qry, ('url-aliases', con.execute(qry).fetchone()[0]))

    # Frequency of aliases
    qry = '''
        WITH 
            alias_freq AS (
                SELECT redir_url, count(*) AS aliases_per_url
                FROM redirs_full_urls
                WHERE redir_type = 'alias'
                GROUP BY redir_url
            )
        SELECT aliases_per_url, count(*)
        FROM alias_freq
        GROUP BY aliases_per_url'''
    for alias_per_url, count in con.execute(qry).fetchall():
        con.execute(ins_qry, (f'url-aliases_{alias_per_url}x', count))

    # Total, unique and average number of internal links
    qry = f'''
        SELECT count(*)
        FROM ed_links
        LEFT JOIN links USING (link_nr)
        WHERE type = 'int' or type = 'ext-to-int' '''
    ed_links_int = con.execute(qry).fetchone()[0]
    qry = f'''
        SELECT count(*)
        FROM links
        WHERE type = 'int' or type = 'ext-to-int' '''
    ed_links_int_uniq = con.execute(qry).fetchone()[0]
    con.execute(ins_qry, ('ed-links_int', ed_links_int))
    con.execute(ins_qry, ('ed-links_int_uniq', ed_links_int_uniq))
    con.execute(ins_qry, ('ed-links_int_avg', ed_links_int / num_pages))

    # Number of internal editorial links that are redirected
    qry = '''
        SELECT count(*)
        FROM ed_links
        LEFT JOIN links AS l USING (link_nr)
        LEFT JOIN redirs AS r ON l.url = r.req_url
        WHERE r.req_url IS NOT NULL
            AND r.redir_type <> 'alias' '''
    links_redirected = con.execute(qry).fetchone()[0]
    con.execute(ins_qry, ('ed-links_int_redir', links_redirected))

    # Number of internal editorial links that do not refer to a wcm url
    qry = '''
        SELECT count(*)
        FROM ed_links
        LEFT JOIN links AS l USING (link_nr)
        LEFT JOIN redirs AS r ON l.url = r.req_url
        WHERE r.req_url IS NOT NULL
            AND r.redir_type = 'alias' '''
    links_non_wcm = con.execute(qry).fetchone()[0]
    con.execute(ins_qry, ('ed-links_int_non-wcm', links_non_wcm))

    # TODO: Number of orphan pages

    # Total, unique and average number of external links
    qry = f'''
        SELECT count(*)
        FROM ed_links
        LEFT JOIN links USING (link_nr)
        WHERE type = 'ext' or type = 'ext-to-ext' '''
    ed_links_ext = con.execute(qry).fetchone()[0]
    qry = f'''
        SELECT count(*)
        FROM links
        WHERE type = 'ext' or type = 'ext-to-ext' '''
    ed_links_ext_uniq = con.execute(qry).fetchone()[0]
    con.execute(ins_qry, ('ed-links_ext', ed_links_ext))
    con.execute(ins_qry, ('ed-links_ext_uniq', ed_links_ext_uniq))
    con.execute(ins_qry, ('ed-links_ext_avg', ed_links_ext / num_pages))

    con.close()
    logging.info('key figures written to the master database')


def dimensions(scrape_db: Path, master_db: Path) -> None:
    """
    **Write dimensional figures from a scrape to the master database.**

    For every combination of the dimensions language, business, category and
    pagetype the number of pages will be written to the dimensions table of
    the master database. Potential previous dimensions from the scrape will
    be deleted before adding the new set.


    Arguments:

        scrape_db: path to the scrape database
        master_db: path to the master database

    Returns:

        None (master database is updated)
    """

    con = sqlite3.connect(master_db, isolation_level=None)
    con.execute(f'ATTACH "{scrape_db}" AS scrape')
    timestamp = parameter(con, 'timestamp')
    con.execute(f"DELETE FROM dimensions WHERE timestamp = '{timestamp}'")

    con.execute(f'''
        INSERT INTO dimensions
            (timestamp, language, business, category, pagetype, pages)
        SELECT '{timestamp}',
            CASE WHEN language IS NULL THEN '' ELSE language END, 
            CASE WHEN business IS NULL THEN '' ELSE business END,
            CASE WHEN category IS NULL THEN '' ELSE category END,
            CASE WHEN pagetype IS NULL THEN '' ELSE pagetype END,
            count(*)
        FROM pages_info
        GROUP BY language, business, category, pagetype
        ORDER BY language DESC, business, category DESC, count(*)''')

    con.close()
    logging.info('dimensions written to the master database')


def compile_history(
        scrape_db: Path,
        master_db: Path,
        check_dates: bool = True) -> bool:
    """
    **Write page changes of a scrape to the master database.**

    History is compiled in the master database for weekly and monthly scrapes
    separately. When check_dates is True (per default) checks are made to
    ensure that history build-up is strictly chronological and for
    consecutive weeks or months respectively. It will be logged when these
    conditions are not met and no history will be added. Skipping these
    checks (when check_dates is False) can disrupt buildup of history in the
    master database and should be used for testing purposes only. Trying to add
    scrape changes preceding the latest in history is always rejected however.

    Arguments:

        scrape_db: path to the scrape database
        master_db: path to the master database
        check_dates: if False skip checking on scrape dates

    Returns:

        true if history was added
    """

    def timestamp_to_dt(ts: str) -> datetime:
        """
        **Convert timestamp to datetime.**

        Validity of the timestamp is not checked.

        Arguments:

            ts: timestamp with format yymmdd-hhmm

        Returns:

            converted timestamp
    """
        return datetime(
            2000 + int(ts[0:2]), int(ts[2:4]),
            int(ts[4:6]),
            int(ts[7:9]), int(ts[9:]))

    def first_dow(dt: datetime) -> datetime:
        """
        **Return start of Monday in same week.**

        Arguments:

            dt: some moment in time

        Returns:

            time at start of monday
        """
        return datetime.fromisocalendar(*dt.isocalendar()[:2], 1)

    def first_dom(dt: datetime) -> datetime:
        """
        **Return start of first day in same month.**

        Arguments:

            dt: some moment in time

        Returns:

            time at start of first day
        """
        return datetime(*dt.timetuple()[:2], 1)

    # Connect databases and get some parameter values
    con = sqlite3.connect(master_db, isolation_level=None)
    con.execute(f'ATTACH "{scrape_db}" AS scrape')
    timestamp = parameter(con, 'timestamp')

    for scrape_type in parameter(con, 'scrape_type').split('/'):
        if scrape_type not in ('weekly', 'monthly'):
            logging.error(
                f'scrape-type invalid to add to history: {scrape_type}')
            return False

        # Test if scrape is after last in history
        qry = f'SELECT max(timestamp) FROM page_hist_{scrape_type}'
        lst_ts = con.execute(qry).fetchone()[0]
        if lst_ts and timestamp <= lst_ts:
            logging.error(
                f'last {scrape_type} scrape in history ({lst_ts}) has a later '
                f'timestamp than this scrape: history not added')
            con.close()
            return False

        # Further tests if date of scrape is valid to add to history
        lst_dt = timestamp_to_dt(lst_ts) if lst_ts else None
        cur_dt = timestamp_to_dt(timestamp)
        if check_dates and scrape_type == 'weekly':
            # Is last registered scrape in previous week?
            if lst_dt:
                if first_dow(lst_dt + timedelta(weeks=1)) != first_dow(cur_dt):
                    logging.warning(
                        f'last weekly scrape in history ({lst_ts}) is not in '
                        'the week previous to this scrape: history is not '
                        'added')
                    con.close()
                    return False
        elif check_dates and scrape_type == 'monthly':
            # Is last registered scrape in previous month?
            if lst_dt:
                if first_dom(lst_dt + timedelta(days=31)) != first_dom(cur_dt):
                    logging.warning(
                        f'last monthly scrape in history ({lst_ts}) is not in '
                        'the month previous to this scrape: history not added')
                    con.close()
                    return False

        # Get relevant aspect names for a page
        qry = 'SELECT name FROM scrape.pragma_table_info("pages_info")'
        page_aspects = [row[0] for row in con.execute(qry).fetchall()
                        if row[0] != 'page_id']

        # Create list of path_id's of new pages (used later to register
        # new pages and exclude them while retrieving changed aspects
        # of existing pages). Note: pages that reappear are not new.
        con.execute('DROP VIEW IF EXISTS new_pages')
        qry = (f'''
            CREATE TEMP VIEW new_pages AS
            WITH
                former_pages AS (
                    SELECT DISTINCT page_id
                    FROM page_hist_{scrape_type}
                )
            SELECT page_id
            FROM scr_wcm_paths
            WHERE page_id NOT IN former_pages''')
        con.execute(qry)

        # Register new pages with life value of 1
        qry = f'''
            INSERT INTO page_hist_{scrape_type}
                (timestamp, page_id, {', '.join(page_aspects)}, life)
            SELECT 
                '{timestamp}', page_id, {', '.join(page_aspects)}, 1
            FROM pages_info
            WHERE page_id IN new_pages
            ORDER BY page_id'''
        con.execute(qry)

        # Negate life value of pages that died
        qry = f'''
            WITH
                latest_life (page_id, life) AS (
                    SELECT DISTINCT
                        page_id,
                        last_value(life) OVER (
                            PARTITION BY page_id
                            ORDER BY
                                CASE WHEN life ISNULL THEN 0 ELSE 1 END,
                                timestamp
                            ROWS BETWEEN UNBOUNDED PRECEDING
                                AND UNBOUNDED FOLLOWING)
                    FROM page_hist_{scrape_type}
                ),
                latest_living AS (
                    SELECT page_id, life
                    FROM latest_life
                    WHERE life > 0
                )
            INSERT INTO page_hist_{scrape_type} (timestamp, page_id, life)
            SELECT '{timestamp}', page_id, -life
            FROM latest_living
            LEFT JOIN pages AS p USING (page_id)
            WHERE p.page_id IS NULL
            ORDER BY page_id'''
        con.execute(qry)

        # Register changed aspects of all pages
        # - new pages are excluded with use of the (temp) view new_pages
        # - reappeared pages are implicitly handled by including pages
        #   that had a previous life value < 0
        # The resulting query is formatted with spacing and linebreaks for
        # debugging purposes (do not alter the string literals in this
        # source).
        # The complete query is documented in a separate sql file.
        qry = '''
            WITH
                latest_hist_aspects AS (
                    SELECT DISTINCT
                        page_id,'''
        for aspect in (*page_aspects, 'life'):
            qry += f'''
                        last_value({aspect}) OVER (
                            PARTITION BY page_id
                            ORDER BY 
                                CASE WHEN {aspect} ISNULL THEN 0 ELSE 1 END,
                                timestamp
                            ROWS BETWEEN UNBOUNDED PRECEDING
                                AND UNBOUNDED FOLLOWING
                        ) AS {aspect},'''
        qry = qry[:-1] + f'''
                    FROM page_hist_{scrape_type}
                ),
                changed_pages AS (
                    SELECT
                        page_id,'''
        for aspect in page_aspects:
            qry += f'''
                        CASE
                            WHEN scr.{aspect} = his.{aspect} 
                            THEN NULL
                            ELSE scr.{aspect}
                        END AS {aspect},'''
        qry += f'''
                        CASE
                            WHEN his.life < 0
                            THEN -his.life + 1
                            ELSE NULL
                        END AS life
                    FROM pages_info AS scr
                    LEFT JOIN latest_hist_aspects AS his USING (page_id)
                    WHERE page_id NOT IN new_pages
                )
            INSERT INTO page_hist_{scrape_type}
            SELECT '{timestamp}', *
            FROM changed_pages
            WHERE '''
        for aspect in page_aspects:
            qry += f'''
                {aspect} NOT NULL OR '''
        qry += '''
                -- Next to include pages that reappear without changes
                life NOT NULL
            ORDER BY page_id'''
        con.execute(qry)

    con.execute('VACUUM')
    con.close()
    logging.info('page history added to the master database')
    return True


def report_scrape(
        scrape_db: Path,
        master_db: Path,
        full_info: bool = True) -> None:
    """
    **Write an Excel report of a weekly or monthly scrape.**

    The report will be written as an Excel workbook to the same directory as
    the scrape database. The type of the scrape is determined by the value of
    the `scrape_type` parameter in the database. In case this type differs
    from `weekly`, `monthly` or a combination of these two connected by a /,
    no report will be written. Existing reports will be overwritten.

    Apart from the sources of the pages, most of the contents of the scrape
    database is comprised in the report. Besides this information the report
    also contains the essential differences for all the site and each new,
    removed and changed page relative to the previous weekly or monthly
    scrape respectively.

    Arguments:

        scrape_db: path to the scrape database
        master_db: path to the master database
        full_info: add sheets with pages, links, redirs and paths

    Returns:

        None
    """

    def shade(row_nr: int, total_rows: int) -> bool:
        """
        **Criterion for shading a row.**

        Depending on the total number of rows the shading will change after
        one, two or three rows.

        Arguments:

            row_nr: actual row
            total_rows: total number of rows

        Returns:

            to shade or not to shade (that's the question)
        """
        if total_rows <= 10:
            height = 1
        elif total_rows < 20:
            height = 2
        else:
            height = 3
        return row_nr % (2 * height) not in list(range(1, height + 1))

    def dict_union(*dicts: dict) -> dict:
        """Return union of the all dictionaries.

        Keys that are common in dictionaries to be merged will contain
        values from the last dictionary.

        Arguments:

            dicts: sequence of dictionaries

        Returns:

            union of all dictionaries
        """
        result = None
        for d in dicts:
            if result:
                result.update(d)
            else:
                result = d.copy()
        return result

    con = sqlite3.connect(master_db, isolation_level=None)
    con_exe = con.execute
    con_exe(f'ATTACH "{scrape_db}" AS scrape')
    scrape_dir = scrape_db.parent

    # Get some parameter values
    timestamp = parameter(con, 'timestamp')
    root_url = parameter(con, 'root_url')

    for scrape_type in parameter(con, 'scrape_type').split('/'):
        if scrape_type not in ('weekly', 'monthly'):
            continue

        # Get timestamp of previous scrape of same type
        qry = f'''
            SELECT max(timestamp)
            FROM page_hist_{scrape_type}
            WHERE timestamp < '{timestamp}' '''
        prev_timestamp = con_exe(qry).fetchone()[0]

        # Initiate the report workbook
        xlsx_file = \
            scrape_dir / f'{timestamp} - {scrape_type} scrape report.xlsx'
        wb = xlsxwriter.Workbook(xlsx_file, {'constant_memory': True})

        # Set formats for the various sheets
        border_color = '#A9A9A9'
        shade_color = '#E8E8E8'
        hdr = {'bold': True, 'font_color': '#FFFFFF', 'fg_color': '#808080',
               'border_color': '#FFFFFF', 'left': 1, 'right': 1}
        val = {'border_color': border_color, 'left': 1, 'right': 1}
        ctr = {'align': 'center'}
        shd = {'fg_color': shade_color}
        del_int = {'num_format': '+0;-0;-'}
        del_flt = {'num_format': '+0.00;-0.00;-'}
        flt = {'num_format': '0.00'}
        fmt_hdr = wb.add_format(hdr)
        fmt_val = wb.add_format(val)
        fmt_val_shd = wb.add_format(dict_union(val, shd))
        fmt_val_flt = wb.add_format(dict_union(val, flt))
        fmt_val_flt_shd = wb.add_format(dict_union(val, flt, shd))
        fmt_val_ctr = wb.add_format(dict_union(val, ctr))
        fmt_val_ctr_shd = wb.add_format(dict_union(val, ctr, shd))
        fmt_val_ctr_flt = wb.add_format(dict_union(val, ctr, flt))
        fmt_val_ctr_flt_shd = wb.add_format(dict_union(val, ctr, flt, shd))
        fmt_val_del_int = wb.add_format(dict_union(val, ctr, del_int))
        fmt_val_del_int_shd = wb.add_format(dict_union(val, ctr, del_int, shd))
        fmt_val_del_flt = wb.add_format(dict_union(val, ctr, del_flt))
        fmt_val_del_flt_shd = wb.add_format(dict_union(val, ctr, del_flt, shd))

        # Add and fill a sheet with scrape and report parameter values
        ws = wb.add_worksheet('Parameters')
        # bug: hide_gridlines(2) on first sheet will hide them for all sheets
        ws.hide_gridlines(0)
        col_spec = [('Name', 17), ('Value', 50)]
        for col in range(len(col_spec)):
            ws.write(0, col, col_spec[col][0], fmt_hdr)
            ws.set_column(col, col, col_spec[col][1])
        qry = 'SELECT name, value FROM parameters'
        parameters = con_exe(qry).fetchall()
        # Add some for the report itself
        parameters += (
            ('report_version', REPORT_VERSION),
            ('report_creation', time.strftime('%b %d, %Y %H:%M'))
        )
        num_pars = len(parameters)
        for row, par in enumerate(parameters, start=1):
            fmt = fmt_val_shd if shade(row, num_pars) else fmt_val
            for col, cell_value in enumerate(par):
                ws.write(row, col, cell_value, fmt)

        # Add and fill a sheet with key figures
        ws = wb.add_worksheet('Key figures')
        ws.hide_gridlines(0)
        col_spec = [('Description', 75), ('Name', 26), ('Value', 9)]
        if prev_timestamp:
            col_spec.append((f'Versus prev. {scrape_type[:-2]}', 20))
        for col in range(len(col_spec)):
            ws.write(0, col, col_spec[col][0], fmt_hdr)
            ws.set_column(col, col, col_spec[col][1])
        ws.freeze_panes('A2')
        if prev_timestamp:
            qry = f'''
                WITH
                    names AS (
                        SELECT DISTINCT name
                        FROM key_figures
                        WHERE timestamp = '{prev_timestamp}'
                            OR timestamp = '{timestamp}'
                    ),
                    prev (name, pv) AS (
                        SELECT name, value
                        FROM key_figures
                        WHERE timestamp = '{prev_timestamp}'
                    ),
                    cur (name, cv) AS (
                        SELECT name, value
                        FROM key_figures
                        WHERE timestamp = '{timestamp}'
                    ),
                    key_values (name, pv, cv) AS (
                        SELECT
                            name,
                            CASE WHEN pv ISNULL THEN 0 ELSE pv END,
                            CASE WHEN cv ISNULL THEN 0 ELSE cv END
                        FROM names
                        LEFT JOIN prev USING (name)
                        LEFT JOIN cur USING (name)
                    )
                SELECT english, name, cv, cv - pv
                FROM key_values
                LEFT JOIN descriptions USING (name)
                ORDER BY seq_nr'''
        else:
            qry = f'''
                SELECT english, name, value
                FROM key_figures
                LEFT JOIN descriptions USING (name)
                WHERE timestamp = '{timestamp}'
                ORDER BY seq_nr'''
        shaded = True
        last_group = ''
        row, col = 0, 0  # to prohibit editor warnings
        for row, qry_result in enumerate(con_exe(qry).fetchall(), start=1):
            # Toggle shading for each separate group of key figures. A group is
            # identified by equal first part of subsequent key figure names. Or
            # by equal second parts when the first part is 'pages_'.
            name_parts = qry_result[1].split('_')
            group = name_parts[0]
            if group in ('pages', 'ed-links') and len(name_parts) > 1:
                group += '_' + name_parts[1]
            if group != last_group:
                shaded = not shaded
                last_group = group
            fmt = fmt_val_shd if shaded else fmt_val
            fmt_flt = fmt_val_flt_shd if shaded else fmt_val_flt
            fmt_del_int = fmt_val_del_int_shd if shaded else fmt_val_del_int
            fmt_del_flt = fmt_val_del_flt_shd if shaded else fmt_val_del_flt
            for col, field in enumerate(qry_result):
                if col == 2:
                    if type(field) == float:
                        ws.write(row, col, field, fmt_flt)
                    else:
                        ws.write(row, col, field, fmt)
                elif col == 3:
                    if type(field) == float:
                        ws.write(row, col, field, fmt_del_flt)
                    else:
                        ws.write(row, col, field, fmt_del_int)
                else:
                    ws.write(row, col, field, fmt)
        ws.autofilter(0, 0, row, col)

        if prev_timestamp:

            # Add and fill a sheet with removed pages
            ws = wb.add_worksheet('Removed pages')
            ws.hide_gridlines(0)
            col_spec = [
                ('page_id', 10), ('Path', 30), ('Title', 30),
                ('Description', 30), ('First h1', 30), ("# h1's", 8),
                ('Language', 11), ('Modified', 14), ('Page type', 15),
                ('Classes', 25), ('Business', 12), ('Category', 11),
                ('Editorial text', 55), ('Automated text', 55)]
            for col in range(len(col_spec)):
                ws.write(0, col, col_spec[col][0], fmt_hdr)
                ws.set_column(col, col, col_spec[col][1])
            ws.freeze_panes('C2')
            prev_scrape_db = str(scrape_db).replace(timestamp, prev_timestamp)
            con_exe(f'ATTACH DATABASE "{prev_scrape_db}" AS prev_scrape')
            qry = f'''
                WITH
                    removed_paths AS (
                        SELECT page_id
                    FROM page_hist_{scrape_type}
                    WHERE timestamp = '{timestamp}'
                        AND life < 0
                    )
                SELECT page_id, path, title, description, first_h1, num_h1s, 
                    language, modified, pagetype, classes, business, category,
                    ed_text, aut_text
                FROM prev_scrape.pages_info
                LEFT JOIN prev_scrape.scr_wcm_paths USING (page_id)
                WHERE page_id IN removed_paths
                ORDER BY page_id'''
            for row, qry_result in enumerate(con_exe(qry).fetchall(), start=1):
                for col, field in enumerate(qry_result):
                    if col in (0, 5, 6, 7, 11):
                        ws.write(row, col, field, fmt_val_ctr)
                    else:
                        ws.write(row, col, field, fmt_val)
            ws.autofilter(0, 0, row, col)
            con_exe('DETACH prev_scrape')

            # Add and fill a sheet with new pages
            ws = wb.add_worksheet('New pages')
            ws.hide_gridlines(0)
            col_spec = [
                ('page_id', 10), ('Path', 30), ('Title', 30),
                ('Description', 30), ('First h1', 30), ("# h1's", 8),
                ('Language', 11), ('Modified', 14), ('Page type', 15),
                ('Classes', 25), ('Business', 12), ('Category', 11),
                ('Editorial text', 55), ('Automated text', 55)]
            for col in range(len(col_spec)):
                ws.write(0, col, col_spec[col][0], fmt_hdr)
                ws.set_column(col, col, col_spec[col][1])
            ws.freeze_panes('C2')
            qry = f'''
                SELECT page_id, path, title, description, first_h1, num_h1s,
                    language, modified, pagetype, classes, business, category,
                    ed_text, aut_text
                FROM page_hist_{scrape_type}
                LEFT JOIN scr_wcm_paths USING (page_id)
                WHERE timestamp = '{timestamp}'
                    AND life = 1
                ORDER BY page_id'''
            for row, qry_result in enumerate(con_exe(qry).fetchall(), start=1):
                for col, field in enumerate(qry_result):
                    if col in (0, 5, 6, 7, 11):
                        ws.write(row, col, field, fmt_val_ctr)
                    else:
                        ws.write(row, col, field, fmt_val)
            ws.autofilter(0, 0, row, col)

            # Add and fill a sheet detailing the aspects of the changed pages
            ws = wb.add_worksheet('Changed aspects')
            ws.hide_gridlines(2)
            col_spec = [
                ('page_id', 10), ('Path', 30), ('Business', 12),
                ('Language', 11), ('Pagetype', 15), ('Aspect', 13),
                ('Current value', 50), ('Modification factor', 20),
                ('Previous value', 50), ('Timestamp previous value', 26)]
            for col in range(len(col_spec)):
                ws.set_column(col, col, col_spec[col][1])
                ws.write(0, col, col_spec[col][0], fmt_hdr)
            ws.freeze_panes('A2')
            # Get relevant aspect names of a page
            qry = f'''
                SELECT name FROM pragma_table_info("page_hist_{scrape_type}")'''
            aspects = [row[0] for row in con_exe(qry).fetchall()
                       if row[0] not in ('timestamp', 'page_id', 'life')]
            qry_results = []
            for aspect in aspects:
                # Get current and previous values of changed aspect
                qry = f'''
                    SELECT
                        page_id, path, scr.business, scr.language, scr.pagetype,
                        '{aspect}' AS aspect,
                        lch.{aspect} AS val_new,
                        his.{aspect} AS val_old,
                        max(his.timestamp) AS ts_old
                    FROM page_hist_{scrape_type} AS lch
                    JOIN page_hist_{scrape_type} AS his USING (page_id)
                    LEFT JOIN scr_wcm_paths USING (page_id)
                    LEFT JOIN pages_info AS scr USING (page_id)
                    WHERE lch.timestamp = '{timestamp}'
                        AND (lch.life IS NULL OR lch.life > 1)
                        AND lch.{aspect} NOT NULL
                        AND his.{aspect} NOT NULL
                        AND his.timestamp < '{timestamp}'
                    GROUP BY page_id'''
                for qry_result in con_exe(qry).fetchall():
                    # Calculate modification factor for relevant aspects...
                    if aspect in ('title', 'description', 'first_h1',
                                  'ed_text', 'aut_text'):
                        new_txt, old_txt = qry_result[6:8]
                        mf = mod_factor(old_txt, new_txt)
                    else:
                        mf = None
                    # ... and insert into the result row.
                    qry_result = list(qry_result)
                    qry_result.insert(7, mf)
                    qry_results.append(qry_result)
            # Sort on page_id and aspect
            qry_results.sort(key=itemgetter(0, 5))
            # Write to sheet
            shaded = True
            last_id = 0
            for row, qry_result in enumerate(qry_results, start=1):
                # Toggle shading on changing page_id
                if last_id != qry_result[0]:
                    shaded = not shaded
                    last_id = qry_result[0]
                fmt = fmt_val_shd if shaded else fmt_val
                fmt_ctr = fmt_val_ctr_shd if shaded else fmt_val_ctr
                fmt_ctr_flt = fmt_val_ctr_flt_shd if shaded else fmt_val_ctr_flt
                for col, field in enumerate(qry_result):
                    if col in (0, 3, 9):
                        ws.write(row, col, field, fmt_ctr)
                    elif col == 7:
                        ws.write(row, col, field, fmt_ctr_flt)
                    else:
                        ws.write(row, col, field, fmt)
            ws.autofilter(0, 0, row, col)

        if full_info:

            # Add and fill sheet with all pages of the scrape
            ws = wb.add_worksheet('All pages')
            ws.hide_gridlines(0)
            col_spec = [
                ('page_id', 10), ('Path', 30), ('Title', 30),
                ('Description', 30), ('First h1', 30), ("# h1's", 8),
                ('Language', 11), ('Modified', 14), ('Page type', 15),
                ('Classes', 25), ('Business', 12), ('Category', 11),
                ('Editorial text', 55), ('Editorial words', 16),
                ('Automated text', 55)]
            for col in range(len(col_spec)):
                ws.write(0, col, col_spec[col][0], fmt_hdr)
                ws.set_column(col, col, col_spec[col][1])
            ws.freeze_panes('C2')
            qry = '''
                SELECT page_id, path, title, description, first_h1, num_h1s,
                    language, modified, pagetype, classes, business, category, 
                    ed_text, aut_text
                FROM pages_info
                LEFT JOIN scr_wcm_paths USING (page_id)
                ORDER BY page_id'''
            for row, qry_result in enumerate(con_exe(qry).fetchall(), start=1):
                ed_text = qry_result[12]
                wrd_cnt = len(re.findall(r'\w+', ed_text))
                qry_result = list(qry_result)
                qry_result.insert(13, wrd_cnt)
                for col, field in enumerate(qry_result):
                    if col in (0, 5, 6, 7, 11, 13):
                        ws.write(row, col, field, fmt_val_ctr)
                    else:
                        ws.write(row, col, field, fmt_val)
            ws.autofilter(0, 0, row, col)

            # Add and fill sheet with all editorial links of the scrape
            ws = wb.add_worksheet('Editorial links')
            ws.hide_gridlines(2)
            col_spec = [
                ('page_id', 10), ('Page path', 75), ('Link text', 50),
                ('link_id', 10), ('Link', 75), ('Link status', 12)]
            for col in range(len(col_spec)):
                ws.write(0, col, col_spec[col][0], fmt_hdr)
                ws.set_column(col, col, col_spec[col][1])
            ws.freeze_panes('A2')
            qry = f'''
                SELECT
                    p1.page_id, p1.path, text, link_id,
                    CASE
                        WHEN url IS NULL
                        THEN p2.path
                        ELSE CASE
                                 WHEN url LIKE '{root_url}%'
                                 THEN replace(url, '{root_url}', '') 
                                 ELSE url
                             END
                    END || 
                    CASE 
                        WHEN anchor IS NULL
                        THEN ''
                        ELSE '#' || anchor
                    END AS link,
                    status
                FROM ed_links
                LEFT JOIN scr_wcm_paths AS p1 USING (page_id)
                LEFT JOIN links USING (link_nr)
                LEFT JOIN scr_wcm_paths AS p2 ON link_id = p2.page_id
                ORDER BY p1.page_id, text'''
            shaded = True
            last_id = 0
            for row, qry_result in enumerate(con_exe(qry).fetchall(), start=1):
                # Toggle shading on changing page_id
                if last_id != qry_result[0]:
                    shaded = not shaded
                    last_id = qry_result[0]
                fmt = fmt_val_shd if shaded else fmt_val
                fmt_ctr = fmt_val_ctr_shd if shaded else fmt_val_ctr
                for col, field in enumerate(qry_result):
                    if col in (0, 3, 5):
                        ws.write(row, col, field, fmt_ctr)
                    else:
                        ws.write(row, col, field, fmt)
            ws.autofilter(0, 0, row, col)

            # Add and fill sheet with all redirects and aliases of the scrape
            ws = wb.add_worksheet('Redirects and aliases')
            ws.hide_gridlines(0)
            col_spec = [
                ('Type', 8), ('req_id', 10), ('Request', 100),
                ('redir_id', 10), ('Redirect', 100)]
            for col in range(len(col_spec)):
                ws.write(0, col, col_spec[col][0], fmt_hdr)
                ws.set_column(col, col, col_spec[col][1])
            ws.freeze_panes('A2')
            qry = '''
                SELECT
                    redir_type, req_id,
                    CASE
                        WHEN req_id IS NULL
                        THEN req_url
                        ELSE req.path
                    END AS req,
                    redir_id,
                    CASE
                        WHEN redir_id IS NULL
                        THEN redir_url
                        ELSE redir.path
                    END AS redir
                FROM redirs AS r
                LEFT JOIN scr_wcm_paths AS req ON r.req_id = req.page_id
                LEFT JOIN scr_wcm_paths AS redir ON r.redir_id = redir.page_id
                ORDER BY req'''
            for row, qry_result in enumerate(con_exe(qry).fetchall(), start=1):
                for col, field in enumerate(qry_result):
                    if col in (0, 1, 3):
                        ws.write(row, col, field, fmt_val_ctr)
                    else:
                        ws.write(row, col, field, fmt_val)
            ws.autofilter(0, 0, row, col)

            # Add and fill a sheet with all relevant paths for this scrape
            ws = wb.add_worksheet('Paths')
            ws.hide_gridlines(2)
            col_spec = [('page_id', 10), ('Path', 200)]
            for col in range(len(col_spec)):
                ws.write(0, col, col_spec[col][0], fmt_hdr)
                ws.set_column(col, col, col_spec[col][1])
            ws.freeze_panes('A2')
            qry = f'SELECT page_id, path FROM scr_wcm_paths'
            for row, qry_result in enumerate(con_exe(qry).fetchall(), start=1):
                for col, field in enumerate(qry_result):
                    if col == 0:
                        ws.write(row, col, field, fmt_val_ctr)
                    else:
                        ws.write(row, col, field, fmt_val)
            ws.autofilter(0, 0, row, col)

        wb.close()
        logging.info(f'{scrape_type} report generated')
        print(f'{scrape_type} scrape report generated')

    con.close()


def mod_factor(ref_text: str, act_text: str) -> float:
    """
    **Calculate the modification factor of a text string.**

    The returned value is a measure of the difference between two texts
    on a scale from 0 (texts are exactly equal) to 1 (texts are
    completely different). The value is calculated as 1 - (SR1 + SR2)/2,
    where SR stands for the similarity ratio as defined in the Python
    standard `difflib` module. SR1 represents the similarity of both
    texts. SR2 is the similarity of the sorted set of words from both
    texts. Averaging these ratios has the effect that changes in both
    wording and phrasing are distinguished from changes in phrasing or
    wording only.

    Arguments:

        ref_text: text acting as reference
        act_text: actual text to compare against the reference

    Returns:

        modification factor in the range of 0 to 1
    """
    sm = difflib.SequenceMatcher(a=ref_text, b=act_text)
    lib_ratio = sm.ratio()
    sm.set_seq1(a=sorted(list(set(ref_text.split()))))
    sm.set_seq2(b=sorted(list(set(act_text.split()))))
    set_ratio = sm.ratio()
    return 1 - (lib_ratio + set_ratio) / 2


def mail_result(to: List[str], msg_body: str) -> None:
    """
    **Send a mail to signal the result of a scrape.**

    The subject of the mail will be 'Scrape finished'.

    In case the config file /etc/scraper/scraper.cfg is not found the function
    returns silently.

    Arguments:

        to: email addresses to send the mail to
        msg_body: body of the message to send

    Returns:

        None
    """
    config_file = Path('/etc/scraper/scraper.cfg')
    if config_file.exists():
        config = configparser.ConfigParser()
        config.read(config_file)
        mail_server = config['mail']['server']
        mail_port = config['mail']['port']
        mail_user = config['mail']['user']
        mail_password = config['mail']['password']
        mail_receiver = to
        message = 'Subject: Scrape finished\n' + msg_body

        # Send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(mail_server, mail_port,
                              context=context) as server:
            server.login(mail_user, mail_password)
            server.sendmail(mail_user, mail_receiver, message)


def create_master_db(db_path: Path) -> None:
    """
    **Create a new master database.**

    No data in the master database is unique and can always be regenerated
    from the individual scrape databases. For this reason this function can
    be used for recreating the complete master database. Especially while
    upgrading the scrape databases this can be really useful. Furthermore
    this is also a way to document the structure of this database.

    On creating a new master database, the contents of the descriptions table
    need to be imported from the descriptions.csv file.

    Remark: If the database already exists, it will be reused. Tables to be
    created will replace already existing ones, losing all their data in the
    process. Contrary to this, the descriptions table will not be replaced
    and will remain unaltered when existing already.

    Arguments:

        db_path: path of the database file to create

    Returns:

        None
    """

    con = sqlite3.connect(db_path, isolation_level=None)
    con.execute('DROP TABLE IF EXISTS mst_wcm_paths')
    con.execute('''
        CREATE TABLE mst_wcm_paths (
            page_id	    INTEGER PRIMARY KEY AUTOINCREMENT,
            path	    TEXT NOT NULL UNIQUE)''')
    con.execute('DROP TABLE IF EXISTS key_figures')
    con.execute('''
        CREATE TABLE key_figures (
            timestamp	TEXT NOT NULL,
            name	    TEXT NOT NULL,
            value	    INTEGER NOT NULL,
            PRIMARY KEY (timestamp, name))''')
    con.execute('''
        CREATE TABLE IF NOT EXISTS descriptions (
            name	    TEXT PRIMARY KEY NOT NULL,
            seq_nr	    INTEGER UNIQUE,
            dutch	    TEXT NOT NULL,
            english	    TEXT NOT NULL)''')
    con.execute('DROP TABLE IF EXISTS dimensions')
    con.execute('''
        CREATE TABLE dimensions (
            timestamp	TEXT NOT NULL,
            language	TEXT NOT NULL,
            business	TEXT NOT NULL,
            category	TEXT NOT NULL,
            pagetype	TEXT NOT NULL,
            pages	    INTEGER NOT NULL,
            PRIMARY KEY (timestamp, language, business, category, pagetype))''')
    for scr_type in ('weekly', 'monthly'):
        con.execute(f'DROP TABLE IF EXISTS page_hist_{scr_type}')
        con.execute(f'''
            CREATE TABLE page_hist_{scr_type} (
                timestamp	TEXT NOT NULL,
                page_id	    INTEGER NOT NULL,
                title	    TEXT,
                description	TEXT,
                num_h1s	    INTEGER,
                first_h1	TEXT,
                language	TEXT,
                modified	DATE,
                pagetype	TEXT,
                classes	    TEXT,
                ed_text	    TEXT,
                aut_text	TEXT,
                business	TEXT,
                category	TEXT,
                life	    INTEGER,
                PRIMARY KEY (timestamp, page_id),
                FOREIGN KEY (page_id)
                    REFERENCES mst_wcm_paths(page_id)
                    ON UPDATE RESTRICT
                    ON DELETE RESTRICT)''')
