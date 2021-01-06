"""Classes and functions for scraping www.belastingdienst.nl (version 2.10).

Classes in this module:

- ScrapeDB: encapsulation of an SQLite scrape database

Functions in this module:

- setup_file_logging: enable uniform logging for all modules
- scrape_page: scrape an html page and create an bs4 representation of it
- page_links: retrieve all links from the body of a page
- content_trees: return two html trees with editorial and automated content
- flatten_tagbranch_to_navstring: reduce complete tag branch to NavigableString
- get_text: retrieve essential editorial and automated text content from a page
- scrape_dirs: generator of scrape directories over a range of timestamps
- update_scrapes_table: update or repopulate the scrapes table in the master db
- page_figures: get typical figures from all pages
- redir_figures: get typical figures from all redirects and aliases
- dimensions: get dimensional totals for a scrape
- master_figures: add typical figures to the master db for a range of scrapes
- compile_history: compile history of page changes within the master database

Module public constants:

- dv_types, bib_types, alg_types: sets of pagetypes that are considered to
    belong to a specific page category
"""

import re
import copy
import logging
import requests
import sqlite3
import zlib
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Union
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag, Comment, Script, Stylesheet

dv_types = {'bld-filter', 'bld-dv-content'}
bib_types = {'bld-bd', 'bld-cluster', 'bld-direction', 'bld-landing',
             'bld-overview', 'bld-sitemap', 'bld-target',
             'bld-targetGroup',
             'bld-concept', 'bld-faq'}
alg_types = {'bld-outage', 'bld-newsItem', 'bld-iahWrapper'}

_re_domain = re.compile(r'^https?://([\w-]*\.)*[\w-]*(?=/)')
_re_path = re.compile(r'^/[^/]')
_re_network_path = re.compile(r'^//[^/]')
_re_protocol = re.compile(r'^[a-z]{3,6}:')


class ScrapeDB:
    """Class encapsulating a scrape database.

    All actions on the scrape database are handled via the class methods.

    Class constants define some of the class behaviour.
    """

    version = '2.8'
    extracted_fields = [
        ('title', 'TEXT'),
        ('description', 'TEXT'),
        ('num_h1s', 'INTEGER'),
        ('first_h1', 'TEXT'),
        ('language', 'TEXT'),
        ('modified', 'DATE'),
        ('pagetype', 'TEXT'),
        ('classes', 'TEXT'),
        ('ed_text', 'TEXT'),
        ('aut_text', 'TEXT')
    ]
    derived_fields = [
        ('business', 'TEXT'),
        ('category', 'TEXT')
    ]

    def __init__(self, db_file, create=False, version_check=True):
        """Initiates the database object that encapsulates a scrape database.

        Writes the database version in the parameters table while creating a
        database. Reports an error if a database is opened with an incompatible
        version.

        Args:
            db_file (Path): name or path of the database file
            create (bool): create & connect database if True, else just connect
            version_check (bool): disable version check if False
        """
        self.db_file = db_file
        self.db_con = sqlite3.connect(self.db_file, isolation_level=None)
        self.exe = self.db_con.execute

        # next pragma's might improve query speed when db is on a network drive
        # (first indication is not positive)
        # self.exe('PRAGMA synchronous = OFF')
        # self.exe('PRAGMA journal_mode = PERSIST')

        if create:
            self.exe('''
                CREATE TABLE pages (
                    page_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path    TEXT NOT NULL UNIQUE,
                    doc     BLOB NOT NULL)''')
            self.exe('CREATE UNIQUE INDEX idx_pages_path ON pages (path)')
            self.exe('''
                CREATE TABLE redirs (
                    req_path   TEXT PRIMARY KEY NOT NULL UNIQUE,
                    redir_path TEXT NOT NULL,
                    type       TEXT)''')
            self.exe('''
                CREATE TABLE ed_links (
                    page_id	  INTEGER NOT NULL,
                    link_text TEXT,
                    link_id   INTEGER,
                    ext_url   TEXT,
                    FOREIGN KEY (page_id, link_id)
                    REFERENCES pages (page_id, page_id)
                        ON UPDATE RESTRICT
                        ON DELETE RESTRICT)''')
            self.exe('''
                CREATE VIEW "ed_links_expl" AS
                    SELECT
                        l.page_id, p1.path AS page_path, l.link_text,
                        l.link_id, p2.path AS link_path, ext_url
                    FROM ed_links AS l
                        JOIN pages AS p1 USING (page_id) 
                        LEFT JOIN pages AS p2 ON link_id = p2.page_id''')
            self.exe('''
                CREATE TABLE parameters (
                    name  TEXT PRIMARY KEY NOT NULL UNIQUE,
                    value TEXT NOT NULL)''')
            self.exe(
                f'INSERT INTO parameters VALUES ("db_version", {self.version})')
            logging.info(f'New scrape.db v{self.version} created')
        else:
            qry = 'SELECT value FROM parameters WHERE name = "db_version"'
            db_version = self.exe(qry).fetchone()[0]
            if version_check and db_version != self.version:
                logging.error(f'Incompatible database version: {db_version}')
                raise sqlite3.DatabaseError(
                    f'Incompatible database version: {db_version}')

    def close(self):
        """Close the connection to the database.

        Returns:
            None
        """
        self.db_con.close()

    def add_page(self, path, doc):
        """Add a scraped page.

        Args:
            path (str): path relative to the root of the scrape
            doc (str): complete scraped html of the page

        Returns:
            int|None: id of the page if not yet saved, None otherwise
        """
        qry = 'INSERT INTO pages (path, doc) VALUES (?, ?)'
        try:
            return self.exe(qry, [path, zlib.compress(doc.encode())]).lastrowid
        except sqlite3.IntegrityError:
            return None

    def get_page(self, path):
        """Get the id and complete doc string of a page.

        Args:
            path (str): path of the page, relative to root of scrape

        Returns:
            (int, str)|None: tuple (page_id, doc_string) or None if no match
        """
        qry = 'SELECT page_id, doc FROM pages WHERE path = ?'
        page = self.exe(qry, [path]).fetchone()
        if page:
            return page[0], zlib.decompress(page[1]).decode()
        else:
            return None

    def pages(self):
        """Generator for all pages of a stored scrape.

        Iterates over the pages stored in the database, yielding a threefold
        tuple containing: the id of the page, the page path (relative to the
        root of the scrape) and the scraped page as string.

        Returns:
            (int, str, str): page_id, page_path, page_string
        """
        qry = 'SELECT page_id, path, doc FROM pages'
        for page_id, path, doc in self.exe(qry):
            yield page_id, path, zlib.decompress(doc).decode()

    def num_pages(self):
        """Get total number of pages.

        Returns:
            int: number of pages
        """
        return self.exe('SELECT count(*) FROM pages').fetchone()[0]

    def add_redir(self, req_path, redir_path, redir_type):
        """Add a redirect that occurred during a page scrape.

        Args:
            req_path (str): requested path
            redir_path (str): path of the redirect
            redir_type (int|str): characterisation of the redirect: status code
                or textual

        Returns:
            None
        """
        qry = 'INSERT INTO redirs (req_path, redir_path, type) VALUES (?, ?, ?)'
        try:
            self.exe(qry, [req_path, redir_path, redir_type])
        except sqlite3.IntegrityError:
            pass
        return None

    def redirs(self):
        """Generator for all redirects of a stored scrape.

        Iterates over the redirects stored in the database, yielding a
        threefold tuple containing: the requested path, the path and the type
        of the redirect. All paths are relative to the root_url of the scrape.

        Returns:
            (str, str, str): requested path, path of redirect, type of redirect
        """
        qry = 'SELECT req_path, redir_path, type FROM redirs'
        for req_path, redir_path, redir_type in self.exe(qry):
            yield req_path, redir_path, redir_type

    def get_def_url(self, req_path):
        """Get the definitive url or path of a page.

        This definitive url or path is deduced via the available redirects in
        the redirs table. In case no redirect is available in the redirs
        table for the requested path, it will be returned unaltered.

        Args:
            req_path (str): requested path relative to root_url

        Returns:
            str: final redirected url or path relative to root_url
        """
        qry = 'SELECT redir_path, type FROM redirs WHERE req_path = ?'
        path = req_path
        while True:
            redir = self.exe(qry, [path]).fetchone()
            if redir:
                path, redir_type = redir
                if redir_type == 'alias':
                    # an alias redirects to the definitive path
                    redir = self.exe(qry, [path]).fetchone()
                    # test if definitive path does not get redirected itself
                    if redir:
                        logging.warning(
                            f'Definitive path gets redirected: {path}')
                    return path
                else:
                    # no alias redir, so maybe still another redir to go
                    continue
            else:
                return path

    def upd_par(self, name, value):
        """Insert or update a parameter.

        Args:
            name (str): name of the parameter
            value (str|int|float): value of the parameter

        Returns:
            None
        """
        qry = 'INSERT OR REPLACE INTO parameters (name, value) VALUES (?, ?)'
        self.exe(qry, [name, value])

    def get_par(self, name):
        """Get the value of a parameter.

        Args:
            name (str): name of the parameter

        Returns:
            str|int|float: value of the parameter
        """
        qry = 'SELECT value FROM parameters WHERE name = ?'
        result = self.exe(qry, [name]).fetchone()
        if not result:
            return None
        value = result[0]
        try:
            value = int(value)
        except ValueError:
            try:
                value = float(value)
            except ValueError:
                pass
        return value

    def repop_ed_links(self):
        """Repopulate links table with editorial links from all pages.

        The links are extracted from each page after pruning the non editorial
        (and automated) content branches from the html tree.
        """

        # purge links table
        self.exe('DELETE FROM ed_links')

        # (re)populate links table
        num_pages = self.num_pages()
        root_url = self.get_par('root_url')
        timestamp = self.get_par('timestamp')
        start_time = time.time()

        logging.info('Populating links table started')

        # cycle over all pages
        page_num = 0
        for page_id, page_path, page_string in self.pages():
            page_num += 1
            soup = BeautifulSoup(page_string, features='lxml')
            editorial_copy, void = content_trees(soup)
            links = page_links(editorial_copy, root_url, root_rel=True)

            # cycle over all links of this page
            for link_text, link_url in links:
                def_url = self.get_def_url(link_url)
                page = self.get_page(def_url)
                if page:
                    # the link points to an internal page
                    link_id = page[0]
                    link_url = None
                else:
                    # because the link destination is not in the pages table,
                    # it is considered external
                    link_id = None
                self.exe('''
                    INSERT INTO ed_links (page_id, link_text, link_id, ext_url)
                    VALUES (?, ?, ?, ?)''',
                         [page_id, link_text, link_id, link_url])

            # print progress and prognosis
            if page_num % 250 == 0:
                page_time = (time.time() - start_time) / page_num
                togo_time = int((num_pages - page_num) * page_time)
                print(f'fetching links from pages of {timestamp} - togo: '
                      f'{num_pages - page_num} pages / '
                      f'{togo_time // 60}:{togo_time % 60:02} min')

        logging.info('Populating links table completed')

    def links(self):
        """Generator for all links of a stored scrape.

        Iterates over the links stored in the database, yielding a fourfold
        tuple:

        - path of the originating page (relative to the root of the scrape)
        - text of the link
        - if internal relative to scrape root, path of the link, else None
        - if external relative to scrape root, full url of the link, else None

        Returns:
            (str, str, str|None, str|None): (page path, link text, link path,
                link url)
        """
        qry = '''
            SELECT page_path, link_text, link_path, ext_url
            FROM ed_links_expl'''
        for page_path, link_text, link_path, ext_url in self.exe(qry):
            yield page_path, link_text, link_path, ext_url

    def page_full_info(self, path):
        """Get all available information of a page.

        The returned dictionary has the next contents:

            - 'page_id': (int) page_id
            - 'path': (str) path
            - 'doc': (str) html source
            - 'title': (str) title
            - 'description': (str) description
            - 'num_h1s': (int) number of h1 tags
            - 'first_h1': (str) text of the first h1 tag
            - 'language': (str) language
            - 'modified': (date) last modification date
            - 'pagetype': (str) type
            - 'classes': (str) classes separated by spaces
            - 'ed_text': (str) newline separated text from editorial content
            - 'aut_text': (str) newline separated text from automated content
            - 'business': (str) 'belastingen', 'toeslagen' or 'douane'
            - 'category': (str) 'dv', 'bib' or 'alg'

        Args:
            path (str): path of the page

        Returns:
            dictionary[str, str|date|None] | None: info name:value pair
        """
        qry = "PRAGMA table_info('pages_full')"
        fields = [r[1] for r in self.exe(qry).fetchall()]
        qry = "SELECT * FROM pages_full WHERE path = ?"
        row = self.exe(qry, [path]).fetchone()
        if row:
            # type hint to prohibit warnings
            info: Dict[str, Union[date, str, bytes, None]]
            info = dict(zip(fields, row))
            mdate = info['modified']
            info['modified'] = date.fromisoformat(mdate) if mdate else None
            info['doc'] = zlib.decompress(info['doc']).decode()
            return info
        else:
            return None

    def pages_full(self):
        """Page generator yielding all available information per page.

        The yielded dictionary has the next contents:

            - 'page_id': (int) page_id
            - 'path': (str) path
            - 'doc': (str) html source
            - 'description': (str) description
            - 'title': (str) title
            - 'num_h1s': (int) number of h1 tags
            - 'first_h1': (str) text of the first h1 tag
            - 'language': (str) language
            - 'modified': (date) last modification date
            - 'pagetype': (str) type
            - 'classes': (str) classes separated by spaces
            - 'ed_text': (str) newline separated text from editorial content
            - 'aut_text': (str) newline separated text from automated content
            - 'business': (str) 'belastingen', 'toeslagen' or 'douane'
            - 'category': (str) 'dv', 'bib' or 'alg'

        Yields:
            dictionary[str, str|date|None]: info name:value pair
        """
        qry = "PRAGMA table_info('pages_full')"
        fields = [r[1] for r in self.exe(qry).fetchall()]
        qry = "SELECT * FROM pages_full"
        for row in self.exe(qry):
            # type hint to prohibit warnings
            info: Dict[str, Union[date, str, bytes, None]]
            info = dict(zip(fields, row))
            mdate = info['modified']
            info['modified'] = date.fromisoformat(mdate) if mdate else None
            info['doc'] = zlib.decompress(info['doc']).decode()
            yield info

    def extract_pages_info(self):
        """Add table with information extracted from all pages.

        Extracted information concerns data that is readily available within
        the page that is stored in the pages table. Storing this data in a
        seperate table is strictly redundant, but serves faster access.

        The fields of the pages_info table are defined by the class constants
        extracted_fields and derived_fields. Besides the pages_info table a
        pages_full view is added that joins the pages table with the
        pages_info table. Existing table and/or view are deleted before
        creating new ones.

        The following information is added for each page:

        - title: content of <title> tag
        - description: content of <meta name="description" content="..." />
        - num_h1s: number <h1>'s
        - first_h1: text of the first h1
        - language: content of <meta name="language" content="xx" />
        - modified: content of <meta name="DCTERMS.modified" content="date" />
        - pagetype: attribute value of <body data-pageType="...">
        - classes: attribute value of <body class="...">
        - ed_text: editorial text of the page
        - aut_text: automated text of the page

        The pages_info table accommodates additional fields to contain
        derived information for each page. This is further detailed in the
        derive_pages_info method of this class.

        It will be logged when tags or attributes are missing or values are
        invalid.
        """

        # create new pages_info table
        self.exe('DROP TABLE IF EXISTS pages_info')
        fields = self.extracted_fields + self.derived_fields
        info_columns = ', '.join([f'{f[0]} {f[1]}' for f in fields])
        self.exe(f'''
            CREATE TABLE pages_info (
                page_id	 INTEGER PRIMARY KEY NOT NULL UNIQUE,
                {info_columns},
                FOREIGN KEY (page_id)
                REFERENCES pages (page_id)
                    ON UPDATE RESTRICT
                    ON DELETE RESTRICT)''')

        # create new pages_full view
        self.exe('DROP VIEW IF EXISTS pages_full')
        self.exe('''
            CREATE VIEW pages_full AS
                SELECT *
                FROM pages
                LEFT JOIN pages_info USING (page_id)''')
        self.exe('VACUUM')
        logging.info(
            'Pages_info table and pages_full view (re)created in scrape.db')

        # extract info from all pages while populating the pages_info table
        num_pages = self.num_pages()
        timestamp = self.get_par('timestamp')
        start_time = time.time()

        logging.info('Extracting info from pages started')

        # cycle over all pages
        page_num = 0
        for page_id, path, page_string in self.pages():
            page_num += 1
            soup = BeautifulSoup(page_string, features='lxml')
            info = {'page_id': page_id}

            # get title
            title = soup.head.title
            if not title:
                logging.warning(f'Page has no <title> tag: {path}')
                title = None
            else:
                title = title.text
                if not title:
                    logging.warning(f'Page with empty title: {path}')
            info['title'] = title

            # get description
            description = soup.head.find(attrs={'name': 'description'})
            if not description:
                # there are more then 800 occurences of this situation
                # TODO: log missing description as warning when this is
                #       a rare exception only
                logging.debug(
                    f'Page has no <meta name="description"/> tag: {path}')
                description = None
            else:
                description = description['content']
                if not description:
                    logging.warning(f'Page with empty description: {path}')
            info['description'] = description

            # get info from <h1> tags
            h1s = []
            for h1 in soup.find_all('h1'):
                h1s.append(h1.text)
            if len(h1s) == 0:
                logging.warning(f'Page without h1: {path}')
            info['num_h1s'] = len(h1s)
            info['first_h1'] = h1s[0] if h1s else None

            # get language
            language = soup.head.find('meta', attrs={'name': 'language'})
            if not language:
                logging.warning(
                    f'Page has no <meta name="language"/> tag: {path}')
                language = None
            else:
                language = language['content']
                if not language:
                    logging.warning(f'Page with empy language: {path}')
            info['language'] = language

            # get date modified
            modified = soup.head.find('meta',
                                      attrs={'name': 'DCTERMS.modified'})
            if not modified:
                logging.warning(
                    f'Page has no tag <meta name="DCTERMS.modified"/>: {path}')
                modified = None
            else:
                try:
                    modified = date.fromisoformat(modified['content'])
                except ValueError:
                    logging.warning(
                        f'Page with improper modification date: {path}')
                    modified = None
            info['modified'] = modified

            # get type of page
            if 'data-pagetype' not in soup.body.attrs:
                logging.warning('Page has no data-pagetype attribute in the '
                                f'<body> tag: {path}')
                pagetype = None
            else:
                pagetype = soup.body['data-pagetype']
                if not pagetype:
                    logging.warning(
                        f'Page with empty pagetype in <body> tag: {path}')
            info['pagetype'] = pagetype

            # get classes
            if 'class' not in soup.body.attrs:
                logging.warning(
                    f'Page has no class attribute in the <body> tag: {path}')
                classes = None
            else:
                classes = soup.body['class']
                if not classes:
                    logging.warning(
                        f'Page with empty class in <body> tag: {path}')
            info['classes'] = ' '.join(classes) if classes else None

            # get editorial and automated texts
            info['ed_text'], info['aut_text'] = get_text(soup)

            # add info to the database
            fields = ', '.join(info)
            qmarks = ('?, ' * len(info))[:-2]
            self.exe(f'INSERT INTO pages_info ({fields}) VALUES ({qmarks})',
                     list(info.values()))

            # print progress and prognosis
            if page_num % 250 == 0:
                page_time = (time.time() - start_time) / page_num
                togo_time = int((num_pages - page_num) * page_time)
                print(
                    f'adding extracted info to scrape database of {timestamp} '
                    f'- togo: {num_pages - page_num} pages / '
                    f'{togo_time // 60}:{togo_time % 60:02} min')

        logging.info('Extracting info from pages completed')

    def derive_pages_info(self):
        """Add derived information for all pages.

        Derived information as such is not available within a page,
        but calculated or interpreted from other information. To derive this
        information, the extracted information should already be available in
        the pages_info table. This can be accomplished by using the
        extract_pages_info method of this class.

        The fields in which the derived info is saved, are already available in
        the pages_info table. In case a field is added, the extract_pages_info
        method can be used to recreate the pages_table, implicitly adding the
        extra fields. The class constants extracted_fields and derived_fields
        define together the fields that are created in the pages_info table.

        The following information is added for each page:

        - business: 'belastingen', 'toeslagen' or 'douane'
        - category: 'dv', 'bib' or 'alg'

        It will be logged when info can not be derived due to inconsistent or
        unavailable information.
        """

        # clear derived info fields in pages_info table
        set_cols = ', '.join([f'{f[0]} = NULL' for f in self.derived_fields])
        self.exe(f'UPDATE pages_info SET {set_cols}')

        # derive info from all pages while updating the pages_info table
        num_pages = self.num_pages()
        timestamp = self.get_par('timestamp')
        start_time = time.time()

        logging.info('Deriving info from pages started')

        # cycle over all pages, with wrapper pages after all others, because
        # the wrapper category is determined by the catogories of pages
        # linking to that wrapper page
        for_qry = '''
            SELECT page_id, pagetype, classes
            FROM pages_info
            ORDER BY
                CASE pagetype
                    WHEN 'bld-wrapper' THEN 2 ELSE 1
                END'''
        page_num = 0
        for page_id, pagetype, classes in self.exe(for_qry).fetchall():
            page_num += 1
            fields = {'page_id': page_id}

            # determine business
            if classes:
                if 'toeslagen' in classes:
                    business = 'toeslagen'
                elif 'douane' in classes:
                    business = 'douane'
                else:
                    business = 'belastingen'
            else:
                business = None
            fields['business'] = business

            # determine category: dv, bib or alg
            if pagetype in dv_types:
                category = 'dv'
            elif pagetype in bib_types:
                category = 'bib'
            elif pagetype in alg_types:
                category = 'alg'
            elif pagetype == 'bld-wrapper':
                cat_qry = '''
                    SELECT category
                    FROM ed_links
                        JOIN pages_info USING (page_id)
                    WHERE link_id = ?
                    GROUP BY category'''
                categories = self.exe(cat_qry, [page_id]).fetchall()
                if len(categories) == 1:
                    # all pages linking to the wrapper are of the same category
                    category = categories[0][0]
                else:
                    category = 'alg'
            else:
                category = 'unknown'
            fields['category'] = category

            # save the derived fields in the pages_info table
            set_str = ', '.join(f'{k} = :{k}' for k in fields)
            self.exe(f'''
                UPDATE pages_info
                SET {set_str}
                WHERE page_id = :page_id''',
                     fields)

            # print progress and prognosis
            if page_num % 500 == 0:
                page_time = (time.time() - start_time) / page_num
                togo_time = int((num_pages - page_num) * page_time)
                print(
                    f'adding derived info to scrape database of {timestamp} - '
                    f'togo: {num_pages - page_num} pages / '
                    f'{togo_time // 60}:{togo_time % 60:02} min')

        logging.info('Deriving info from pages completed')


def setup_file_logging(directory, log_level=logging.INFO):
    """Enable uniform logging for all modules.

    Args:
        directory (Path): path of the directory of the logfile
        log_level (int): the lowest severity level that will be logged
            symbolic values are available from the logging module:
            CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET

    Returns:
        None
    """
    logging.basicConfig(
        filename=str(directory / 'log.txt'),
        format='[%(asctime)s] %(levelname)-8s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=log_level,
        force=True)


def scrape_page(root_url, req_url):
    """Scrape an html page.

    Since there can be more than one redirect per requested page, the last
    element in the return tuple is a list of redirects instead of a single
    redirect (a 301 is often followed by a 302).
    The first item in the returned tuple is the url that comes from the content
    attribute of the <meta name="DCTERMS.identifier"> tag. This is the
    definitive url as generated by the WCM system.
    All url's are absolute.

    Args:
        root_url (str): url that will be treated as the base of the scrape;
            links starting with root_url are interpreted as within scope
        req_url (str): url of requested page

    Returns:
        (str, BeautifulSoup, str, list of (str, str, str)):
            definitive url of the page,
            bs4 representation of the page,
            complete page as string,
            list of (requested url, url of the response, type of redirect)
    """
    redirs = []

    while True:
        # cycle until no rewrites or redirects
        resp = requests.get(req_url)
        if resp.status_code != 200:
            logging.error(f'Unexpected response from {req_url}; '
                          f'status code is {resp.status_code}.')
            raise requests.RequestException

        # read and parse the response into a soup document
        # resp_url = resp.url
        page_as_string = resp.text
        soup = BeautifulSoup(page_as_string, features='lxml')

        # are there any redirects?
        if len(resp.history) != 0:
            i_req_url = req_url
            for i_resp in resp.history:
                if i_req_url.startswith(root_url):
                    # requested page is within scope of scrape
                    i_resp_url = i_resp.headers['Location']
                    redirs.append((i_req_url, i_resp_url, i_resp.status_code))
                    if i_resp.status_code not in (301, 302):
                        # just to know if this occurs; probably not
                        logging.warning(
                            f'Requesting {i_req_url} responded with '
                            f'status code {i_resp.status_code}.')
                    i_req_url = i_resp_url

        # do we have a client-side redirect page via the next header tag?
        #       <meta http-equiv="refresh" content="0;url=...">
        meta_tag = soup.head.find('meta', attrs={'http-equiv': 'refresh'})
        if meta_tag:
            resp_url = meta_tag['content'].partition('url=')[2]
            # complete url if necessary
            if re.match(_re_path, resp_url):
                domain = _re_domain.match(root_url)[0]
                resp_url = domain + resp_url
            elif re.match(_re_network_path, resp_url):
                protocol = _re_protocol.match(root_url)[0]
                resp_url = protocol + resp_url
            redirs.append((req_url, resp_url, 'client'))
            req_url = resp_url
            continue

        resp_url = resp.url
        if resp_url.startswith(root_url):
            # response url is within scope
            meta_tag = soup.head.find(
                'meta', attrs={'name': 'DCTERMS.identifier'})
            if meta_tag:
                def_url = meta_tag['content']
                # complete path
                if def_url.startswith('/wps/wcm/connect'):
                    domain = _re_domain.match(root_url)[0]
                    def_url = domain + def_url
                    if def_url != resp_url:
                        redirs.append((resp_url, def_url, 'alias'))
                else:
                    logging.warning('Non-standard definitive url; '
                                    f'falling back to: {resp_url}')
                    def_url = resp_url
            else:
                logging.error(
                    f'Page without definitive url; falling back to: {resp_url}')
                def_url = resp_url
        else:
            def_url = resp_url

        # return implicitly ends while loop
        return def_url, soup, page_as_string, redirs


def page_links(soup, root_url, root_rel=False, remove_anchor=True):
    """Retrieve all links from a BeautifulSoup document.

    The returned links will be absolute url's or paths relative to root_url,
    and will be filtered according to the following criteria:

    - no links from <div id="bld-nojs">
    - no links containing 'readspeaker' or 'adobe'
    - only links that start with /, // or protocol: (uri-scheme)

    Further processing of the links is determined by the arguments:

    - anchors are removed if remove_anchor == True
    - links are relative to root_url if root_rel == True, otherwise absolute

    Warning: the soup document is used destructively!

    Args:
        soup (BeautifulSoup): destructively used bs4 document
        root_url (str): the root url with which the page was scraped
        root_rel (bool): return links relative to root_url
        remove_anchor (bool): remove anchors from links

    Returns:
        list of (str, str): list of (link text, link url) tuples
    """

    # clear branches from which links are excluded
    div_nojs = soup.find('div', id='bld-nojs')
    if div_nojs:
        div_nojs.extract()

    # get links from remaining soup doc
    links = []
    if soup.body and soup.body.a:
        for a_tag in soup.body.find_all('a', href=True):
            link = a_tag['href']
            if 'readspeaker' in link or 'adobe' in link:
                continue
            if remove_anchor:
                link = link.partition('#')[0]

            # make link into complete url's if necessary
            if re.match(_re_path, link):
                domain = _re_domain.match(root_url)[0]
                link = domain + link
            elif re.match(_re_network_path, link):
                protocol = _re_protocol.match(root_url)[0]
                link = protocol + link
            elif not re.match(_re_protocol, link):
                continue

            # make link relative to root_url if needed
            if root_rel:
                link = re.sub(root_url, '', link)

            if link:
                links.append((a_tag.text.strip(), link))

    return links


def content_trees(soup):
    """Return two html trees with only editorial and automated content.

    Both trees return only those tags that represent editorial or automated
    content. All other tags are removed from the originating (soup) tree.
    Because this removal is destructive, the returned trees are copied from the
    originating tree.

    More specifically, next tags are removed from both trees:

    - <head>
    - <header>
    - <footer>
    - <div id="bld-nojs">: for situation that javascript is not active
    - <div class="bld-subnavigatie">: left side navigation of bib pages
    - <div class="bld-feedback">: bottom feedback of content page
    - readspeaker buttons
    - modal dialog for the virtual assistant

    Furthermore the next tag is removed from the editorial tree:

    - <div class="add_content">: content added without editor intervention

    The automated tree will contain the next tags:

    - <div class="add_content">: automatically added content
    - all remaining tags from pages with type bld-overview

    Args:
        soup (BeautifulSoup): bs4 representation of a page

    Returns:
        (BeautifulSoup, BeautifulSoup): (editorial tree, automated tree)"""

    # make working copy from soup doc, because of destructive tag removal
    tree = copy.copy(soup)

    # removal actions for both trees:

    # remove head, header and footer branches
    tree.head.extract()
    if tree.body.header:
        tree.body.header.extract()
    if tree.body.footer:
        tree.body.footer.extract()

    # remove content for non active javascript
    div_nojs = tree.find('div', id='bld-nojs')
    if div_nojs:
        div_nojs.extract()

    # remove sub-navigation
    div_subnav = tree.find(class_='bld-subnavigatie')
    if div_subnav:
        div_subnav.extract()

    # remove feedback
    div_feedback = tree.find(class_='bld-feedback')
    if div_feedback:
        div_feedback.extract()

    # remove readspeaker buttons
    for tag in tree.find_all('div', class_='rs_skip'):
        tag.extract()

    # remove modal dialog for the virtual assistant
    for tag in tree.find_all('div', id='vaModal'):
        tag.extract()

    # further actions per tree:

    # test if page is generated without any editor intervention
    if tree('body', attrs={'data-pagetype': 'bld-overview'}):
        # editor tree becomes trunk only
        ed_tree = BeautifulSoup('<html></html>', features='lxml')
        # automated tree gets all remaining content
        aut_tree = tree
    else:
        # make html trunk for automated tree
        aut_tree = BeautifulSoup('<html></html>', features='lxml')
        # graft content_add branches onto this html trunk
        for tag in tree.find_all('div', class_='content_add'):
            aut_tree.html.append(tag)
        # editor tree gets all
        ed_tree = tree
        # remove (automated) content_add from editor tree
        for tag in ed_tree.find_all('div', class_='content_add'):
            tag.extract()

    return ed_tree, aut_tree


def flatten_tagbranch_to_navstring(tag: Tag):
    """Reduce a complete tag branch to one NavigableString.

    The reduction is realised within the BeautifulSoup data structure that
    the tag is part of. This means that the function replaces the tag branche
    (in place) into a single NavigableString containing all text of the tag
    branch.

    The function uses a recursive tree traversal with a NavigableString as
    leaf. Each instance of the function will combine the text content of all
    children into one NavigableString. Within this string all <br> tags are
    replaced by '#br#' markers. The text content of all former <p>, <h1>,
    <h2>, <h3>, <li> and <div> tags in the tag branche is enclosed with two
    '#br#' markers in this resulting NavigableString.

    The '#br#' markers in the resulting NavigableString act as seperators
    between logical chunks of text. Potentially there can be more consequetive
    '#br#' markers, which has no real significance.

    Args:
        tag (Tag): part of BeautifulSoup structure that will be reduced

    Returns:
        None (tag is replaced in place with one NavigableString)
    """

    # final leaf cases; done with this branch
    if type(tag) in {NavigableString, Comment, Script, Stylesheet}:
        return

    # has this tag children other then NavigableStrings?
    tag_children = list(tag.children)
    child_types = {type(c) for c in tag_children}
    if tag_children and child_types != {NavigableString}:
        # flatten all child branches to NavigableStrings
        for c in tag_children:
            flatten_tagbranch_to_navstring(c)

    # at this point all children (if any) of tag are NavigableStrings
    tag_name = tag.name
    if tag_name == 'br':
        tag.replace_with('#br#')
    elif tag_name == 'a':
        tag.replace_with(f' {tag.text}')  # the leading space is significant
    elif tag_name in {'p', 'h1', 'h2', 'h3', 'li', 'div'}:
        tag_text = tag.text
        tag.replace_with(f'#br#{tag_text}#br#')
    else:
        tag.replace_with(tag.text)

    return


def get_text(soup):
    """Retrieve essential editorial and automated text content from a page.

    The editorial and automated text of the page content is returned together
    as a twofold tuple. Basically the texts are retrieved from a copy of the
    soup document which is pruned back to its editorial or automated content
    branches. Whitespace of these texts is normalised and coherent chunks are
    seperated by newlines.

    Args:
        soup (BeautifulSoup): bs4 representation of a page

    Returns:
        (str, str): (editorial text, automated text) of the page
    """

    result = []

    for tree in content_trees(soup):

        flatten_tagbranch_to_navstring(tree.html)

        # replace non-breaking spaces with normal ones
        txt = tree.text.replace(b'\xc2\xa0'.decode(), ' ')

        # subsitute one space for any cluster of whitespace chars (getting rid
        # of returns, newlines, tabs, spaces, etc.; this is html, you know!)
        txt = re.sub(r'\s+', ' ', txt)

        # change marked <br>'s to newlines, while reducing multiples
        # seperated by whitespace only; the final strip() removes potential
        # trailing newlines
        txt = re.sub(r'\s*(#br#\s*)+\s*', r'\n', txt).strip()

        result.append(txt)

        # remove the working copy of the soup doc
        tree.decompose()

    return result


def scrape_dirs(master_dir, min_timestamp='000000-0000',
                max_timestamp='991231-2359', frequency=''):
    """Generator of time ordered scrape directories in given time(stamp)span.

    Timestamps should conform to 'yymmdd-hhmm'.

    Because the algorithm to identify weekly or monthly scrapes is quite
    cumbersome, this function uses the information in the scrapes table of
    the master database, where each scrape is labeled with the periodicity of
    it. To be sure that all scrapes are in this table, it is updated before
    using it.

    This approach is slightly over the top when cycling over all available
    scrapes (so including weekly, monthly and any other scrape). For this
    reason earlier and simpler code to cycle over all scrapes is included as
    inline comment in the body of this function.

    Yield tuples of timestamp and scrape directoriy.

    Args:
        master_dir (Path): directory holding the scrapes
        min_timestamp (str): earliest timestamp of scrapes to include
        max_timestamp (str): latest timestamp of scrapes to include
        frequency (str): 'w' for weekly, 'm' for monthly and '' for all scrapes

    Yields:
        Tuple[str, Path]: (timestamp, scrape directory)
    """

    # historic code when cycling over weekly of monthly scrapes is not needed
    # --------------------------------------------------------------------------
    # dirs = sorted(
    #     [d for d in master_dir.glob('??????-???? - bd-scrape') if d.is_dir()])
    # for scrape_dir in dirs:
    #     timestamp = scrape_dir.name[:11]
    #     if timestamp < min_timestamp or timestamp > max_timestamp:
    #         # scrape is not within timestamp range; get next one
    #         continue
    #     yield timestamp, scrape_dir
    # --------------------------------------------------------------------------

    update_scrapes_table(master_dir)

    mdb_file = master_dir / 'scrape_master.db'
    mdb = sqlite3.connect(mdb_file, isolation_level=None)

    if not frequency:
        periodicity = '%'
    elif frequency in ('w', 'm'):
        periodicity = frequency
    else:
        raise ValueError(f'invalid frequency: {frequency}')

    qry = f'''
        SELECT timestamp
        FROM scrapes 
        WHERE periodicity LIKE '{periodicity}'
            AND timestamp > '{min_timestamp}'
            AND timestamp < '{max_timestamp}'
        ORDER BY timestamp'''

    for scrape in mdb.execute(qry).fetchall():
        timestamp: str = scrape[0]
        scrape_dir = master_dir / (timestamp + ' - bd-scrape')
        yield timestamp, scrape_dir


def update_scrapes_table(master_dir, check_db=True, repop=False):
    """Update or repopulate the scrapes table in the master database.

    To be sure that non regular scrapes do not disrupt any process,
    this function handles various special situations (while regrettably
    adding quite some complexity to the code). Only one case is not
    explicitly handled: adding a scrape before one that is already in the
    scrapes table. For that situation this function can be used however with
    the repop parameter set to True.

    Exceptions are raised in the next (non standard) situations:

        - weekly or monthly scrape is missing
        - weekly scrape is not on Monday or Tuesday
        - monthly scrape is not on one of the first three days
        - scrape directory does not contain a scrape db (when check_db is true)
        - timestamp of directory  and db do not match (when check_db is true)

    Args:
        master_dir (Path): directory containing scrapes and master database
        check_db (bool): check existence and timestamp of scrape db
        repop (bool): repopulate the table by purging it before updating

    Returns:
        None
    """

    def dt_from_timestamp(timestamp):
        """Convert timestamp to datetime.

        Validity of the timestamp is not checked.

        Args:
            timestamp (str): format yymmdd-hhmm

        Returns:
            datetime
        """
        return datetime(
            2000 + int(timestamp[0:2]), int(timestamp[2:4]),
            int(timestamp[4:6]),
            int(timestamp[7:9]), int(timestamp[9:]))

    def first_dow(dt):
        """Return start of Monday in same week.

        Args:
            dt (datetime): some moment in time

        Returns:
            datetime
        """
        return datetime.fromisocalendar(*dt.isocalendar()[:2], 1)

    def first_dom(dt):
        """Return start of first day in same month.

        Args:
            dt (datetime): some moment in time

        Returns:
            datetime
        """
        return datetime(*dt.timetuple()[:2], 1)

    sow_window = 2          # start of week window for weekly scrape
    som_window = 3          # start of month window for monthly scrape
    mdb_file = master_dir / 'scrape_master.db'
    mdb = sqlite3.connect(mdb_file, isolation_level=None)

    last_weekly_monday = None       # Monday in week of last weekly scrape
    last_monthly_firstday = None    # first day in month of last monthly scrape
    last_scrape_timestamp = ''      # timestamp of last scrape

    if repop:
        # purge table
        mdb.execute('DELETE FROM scrapes')
    else:
        # get last registered weekly and monthly scrapes
        qry = '''
            SELECT max(timestamp), periodicity
            FROM scrapes
            GROUP BY periodicity'''
        for timestamp, periodicity in mdb.execute(qry).fetchall():
            if periodicity == 'w':
                last_weekly_monday = first_dow(dt_from_timestamp(timestamp))
            elif periodicity == 'm':
                last_monthly_firstday = first_dom(dt_from_timestamp(timestamp))
            elif periodicity != '':
                raise ValueError(f"periodicity '{periodicity}' is invalid")
            last_scrape_timestamp = max(last_scrape_timestamp, timestamp)

    dir_patt = r'2\d[01]\d[0-3]\d-[0-2]\d[0-5]\d - bd-scrape'
    dirs = sorted([d for d in master_dir.iterdir()
                   if d.is_dir()
                   and re.fullmatch(dir_patt, d.name)
                   and d.name[:11] > last_scrape_timestamp])

    ins_qry = f'''
        INSERT INTO scrapes (timestamp, week, dow, periodicity)
        VALUES (?, ?, ?, ?)'''

    for scrape_dir in dirs:
        scr_timestamp = scrape_dir.name[:11]

        # check existance and timestamp of database
        if check_db:
            sdb_file = scrape_dir / 'scrape.db'
            if not sdb_file.exists():
                raise ValueError(f"no 'scrape.db' in '{scrape_dir}'")
            sdb = ScrapeDB(sdb_file)
            if scr_timestamp != sdb.get_par('timestamp'):
                raise LookupError(
                    f"timestamp '{sdb.get_par('timestamp')}' "
                    f"inconsistent with database in {scrape_dir}")
            sdb.close()

        scr_moment = dt_from_timestamp(scr_timestamp)
        scr_year, scr_week, scr_dow = scr_moment.isocalendar()

        # weekly scrape?
        if not last_weekly_monday:
            # no previous weekly scrape registered
            if scr_dow <= sow_window:
                # first valid weekly scrape
                mdb.execute(ins_qry, [scr_timestamp, scr_week, scr_dow, 'w'])
                last_weekly_monday = first_dow(scr_moment)
                continue  # handle nex scrape_dir
        else:
            # previous weekly scrape registered
            next_weekly_monday = last_weekly_monday + timedelta(days=7)
            nextnext_weekly_monday = next_weekly_monday + timedelta(days=7)
            if next_weekly_monday < scr_moment < nextnext_weekly_monday:
                if scr_dow <= sow_window:
                    # next valid weekly scrape
                    mdb.execute(
                        ins_qry, [scr_timestamp, scr_week, scr_dow, 'w'])
                    last_weekly_monday = first_dow(scr_moment)
                    continue  # handle nex scrape_dir
                else:
                    raise ValueError(
                        f'scrape of {scr_timestamp} is too far in week to be '
                        f'a valid weekly scrape after '
                        f'{last_weekly_monday.strftime("%y%m%d-%H%M")}')
            elif scr_moment >= nextnext_weekly_monday:
                raise ValueError(
                    f'missing weekly scrape; first one available after '
                    f'{last_weekly_monday.strftime("%y%m%d-%H%M")} '
                    f'is {scr_timestamp}')

        # monthlyy scrape?
        if not last_monthly_firstday:
            # no previous monthly scrape registered
            if scr_moment.day <= som_window:
                # first valid monthly scrape
                mdb.execute(ins_qry, [scr_timestamp, scr_week, scr_dow, 'm'])
                last_monthly_firstday = first_dom(scr_moment)
                continue  # handle nex scrape_dir
        else:
            # previous monthly scrape registered
            next_monthly_firstday = datetime(
                *(last_monthly_firstday + timedelta(31)).timetuple()[:2], 1)
            nextnext_monthly_firstday = datetime(
                *(next_monthly_firstday + timedelta(31)).timetuple()[:2], 1)
            if next_monthly_firstday < scr_moment < nextnext_monthly_firstday:
                if scr_moment.day <= som_window:
                    # next valid montly scrape
                    mdb.execute(
                        ins_qry, [scr_timestamp, scr_week, scr_dow, 'm'])
                    last_monthly_firstday = first_dom(scr_moment)
                    continue  # handle nex scrape_dir
                else:
                    raise ValueError(
                        f'scrape of {scr_timestamp} is too far in month to be '
                        f'a valid monthly scrape after '
                        f'{last_monthly_firstday.strftime("%y%m%d-%H%M")}')
            elif scr_moment >= nextnext_monthly_firstday:
                raise ValueError(
                    f'missing monthly scrape; first one available after '
                    f'{last_monthly_firstday.strftime("%y%m%d-%H%M")} '
                    f'is {scr_timestamp}')

        # reaching this point the scrape is neither weekly nor monthly
        # register as ad-hoc
        mdb.execute(ins_qry, [scr_timestamp, scr_week, scr_dow, ''])

    return


def page_figures(database, table):
    """Get typical figures from all pages.

    Next number of pages will be returned:

    - pages: all pages
    - pages_lang_<language>: per <language>
    - pages_buss_<business>: per <business>
    - pages_cat_<category>: per <category>
    - pages_type_<pagetype>: per <pagetype>
    - pages_h1_multi: with more than one h1-tag
    - pages_h1_multi_<pagetype>: with more than one h1-tag per <pagetype>
    - pages_h1_no: without h1-tag
    - pages_title_no: without or with empty title-tag
    - pages_title_duplicate: with non-unique title-tag
    - pages_descr_no: without or with empty description meta-tag
    - pages_descr_long: with description meta-tag longer than 160 characters

    Args:
        database (Path): scrape database
        table (str): table to query

    Returns:
        list[tuple[str, int]]: list of name/value pairs for each typical figure
    """
    db_conn = sqlite3.connect(database, isolation_level=None)
    db_exe = db_conn.execute
    figures = []

    # total pages
    qry = f'''
        SELECT count(*)
        FROM {table}'''
    figures.append(['pages', db_exe(qry).fetchone()[0]])

    # pages per language
    qry = f'''
        SELECT language, count(*)
        FROM {table} 
        GROUP BY language
        ORDER BY language DESC'''
    for language, count in db_exe(qry).fetchall():
        figures.append([f'pages_lang_{language}', count])

    # pages per business
    qry = f'''
        SELECT business, count(*)
        FROM {table} 
        GROUP BY business
        ORDER BY business'''
    for business, count in db_exe(qry).fetchall():
        figures.append([f'pages_buss_{business}', count])

    # pages per category
    qry = f'''
        SELECT category, count(*)
        FROM {table} 
        GROUP BY category
        ORDER BY business DESC'''
    for category, count in db_exe(qry).fetchall():
        figures.append([f'pages_cat_{category}', count])

    # pages per type
    qry = f'''
        SELECT pagetype, count(*)
        FROM {table}
        GROUP BY pagetype
        ORDER BY category DESC, count(*) ASC'''
    for pagetype, count in db_exe(qry).fetchall():
        figures.append([f'pages_type_{pagetype}', count])

    # pages with more than one h1's
    qry = f'''
        SELECT count(*)
        FROM {table}
        WHERE num_h1s > 1'''
    figures.append(['pages_h1_multi', db_exe(qry).fetchone()[0]])

    # pages per type with more than one h1's
    qry = f'''
        SELECT pagetype, count(*)
        FROM {table}
        WHERE num_h1s > 1
        GROUP BY pagetype'''
    for pagetype, count in db_exe(qry).fetchall():
        figures.append([f'pages_h1_multi_{pagetype}', count])

    # pages with no h1
    qry = f'''
        SELECT count(*) 
        FROM {table} 
        WHERE num_h1s = 0'''
    figures.append(['pages_h1_no', db_exe(qry).fetchone()[0]])

    # pages without title
    qry = f'''
        SELECT count(*) 
        FROM {table} 
        WHERE title = '' or title is NULL'''
    figures.append(['pages_title_no', db_exe(qry).fetchone()[0]])

    # pages with non unique title
    qry = f'''
        SELECT sum(c) FROM 
            (SELECT count(*) as c
            FROM {table}
            GROUP BY title)
        WHERE c > 1'''
    figures.append(['pages_title_dupl', db_exe(qry).fetchone()[0]])

    # pages without description
    qry = f'''
        SELECT count(*) 
        FROM {table} 
        WHERE description = '' or description is NULL'''
    figures.append(['pages_descr_no', db_exe(qry).fetchone()[0]])

    # pages with description longer than 160 characters
    qry = f'''
        SELECT count(*) 
        FROM {table} 
        WHERE length(description) > 160'''
    figures.append(['pages_descr_long', db_exe(qry).fetchone()[0]])

    db_conn.close()
    return figures


def redir_figures(database, table):
    """Get typical figures from all redirects and aliases.

    Next figures will be returned:

    - redirs: total number of all redirects
    - redirs_<type>: number of redirects per type
    - redits_<type>_slash: redirects per type with only differing a slash
    - url-aliases: number of url's that alias an authoritative url henk
    - url-aliases_<num>x: number of url's with <num> aliases

    Args:
        database (Path): scrape database
        table (str): table to query

    Returns:
        list[tuple[str, int]]: list of name/value pairs for each typical figure
    """
    db_conn = sqlite3.connect(database, isolation_level=None)
    db_exe = db_conn.execute
    figures = []

    # total redirects (an alias is strictly no redirect)
    qry = f'''
        SELECT count(*)
        FROM {table}
        WHERE type != "alias"'''
    figures.append(('redirs', db_exe(qry).fetchone()[0]))

    # redirects per type
    qry = f'''
        SELECT type, count(*)
        FROM {table}
        WHERE type != 'alias'
        GROUP BY type'''
    for redir_type, count in db_exe(qry).fetchall():
        figures.append((f'redirs_{redir_type}', count))

    # redirects per type that only add or loose the last slash
    qry = f'''
        SELECT type, count(*)
        FROM {table}
        WHERE req_path || '/' = redir_path or req_path = redir_path || '/'
        GROUP BY type'''
    for redir_type, count in db_exe(qry).fetchall():
        figures.append((f'redirs_{redir_type}_slash', count))

    # total aliases
    qry = f'''
        SELECT count(*)
        FROM {table}
        WHERE type = "alias"'''
    figures.append(('url-aliases', db_exe(qry).fetchone()[0]))

    # url's with aliases
    qry = f'''
        SELECT alias_per_url, count(alias_per_url)
        FROM (SELECT redir_path, count(*) as alias_per_url
            FROM {table}
            WHERE type = 'alias'
            GROUP BY redir_path)
        GROUP BY alias_per_url'''
    for alias_per_url, count in db_exe(qry).fetchall():
        figures.append((f'url-aliases_{alias_per_url}x', count))

    db_conn.close()
    return figures


def dimensions(database, table):
    """Get dimensional totals for a scrape.

    From the given database/table number of pages will be returned for every
    combination of the four dimension (language, business, category and
    pagetype).

    Args:
        database (Path): scrape database
        table (str): table to query

    Returns:
        list[list[str, str, str, str, int]]: list of every
            combination of language, business, category, pagetype, number
            of pages (for that combination)
    """
    db_conn = sqlite3.connect(database, isolation_level=None)
    db_exe = db_conn.execute
    dims = []

    qry = f'''
        SELECT language, business, category, pagetype, count(*)
        FROM {table}
        GROUP BY language, business, category, pagetype
        ORDER BY language DESC, business, category DESC, count(*) ASC'''
    for values in db_exe(qry).fetchall():
        values = ['' if v is None else v for v in values]
        dims.append(values)

    db_conn.close()
    return dims


def master_figures(master_dir, min_timestamp, max_timestamp):
    """Add key and dimensional figures to the master db for a range of scrapes.

    Key and dimensional figures will be generated for all the scrapes within
    the given range and (re)written to the scrape_master database.

    Args:
        master_dir (Path): directory containing master db and scrapes
        min_timestamp (str): scrapes before are not processed
        max_timestamp (str): scrapes after are not processed

    Returns:
        None
    """

    mdb_file = master_dir / 'scrape_master.db'
    mdb_conn = sqlite3.connect(mdb_file, isolation_level=None)
    mdb_exe = mdb_conn.execute

    # cycle over scrapes
    for timestamp, scrape_dir in scrape_dirs(master_dir, min_timestamp,
                                             max_timestamp):
        sdb_file = scrape_dir / 'scrape.db'

        key_figures = page_figures(sdb_file, 'pages_info')
        key_figures += redir_figures(sdb_file, 'redirs')
        for name, value in key_figures:
            qry = f'''
                INSERT OR REPLACE INTO key_figures
                    (timestamp, name, value) 
                VALUES ('{timestamp}', '{name}', {value})'''
            mdb_exe(qry)

        dim_figures = dimensions(sdb_file, 'pages_info')
        for language, business, category, pagetype, pages in dim_figures:
            dim_qry = f'''
                INSERT OR REPLACE INTO dimensions
                    (timestamp, language, business, category, pagetype, pages)
                VALUES ('{timestamp}', '{language}', '{business}', 
                    '{category}', '{pagetype}', {pages})'''
            mdb_exe(dim_qry)

        print(f'Typical figures saved to master database '
              f'for scrape of {timestamp}')

    mdb_conn.close()


def compile_history(master_dir, max_timestamp,
                    weekly=True, monthly=True, renew_tables=False):
    """Compile history of page changes within the master database.

    Args:
        master_dir (Path): directory containing master db and scrapes
        max_timestamp (str): only scrapes before are processed
        weekly (bool): compile weekly history
        monthly (bool): compile monthly history
        renew_tables (bool): refresh complete history

    Returns:
        None
    """

    # connect master database
    mdb_file = master_dir / 'scrape_master.db'
    mdb = sqlite3.connect(mdb_file, isolation_level=None)
    mdb_exe = mdb.execute

    # path table has to be present
    paths_table_exists = mdb_exe('''
        SELECT name
        FROM sqlite_master
        WHERE type = "table"
          AND name = "paths"''').fetchone()
    if not paths_table_exists:
        mdb_exe('''
            CREATE TABLE paths (
                path_id	    INTEGER PRIMARY KEY AUTOINCREMENT,
                path	    TEXT NOT NULL UNIQUE)''')

    for do, freq in [(weekly, 'weekly'), (monthly, 'monthly')]:
        if not do:
            continue

        hist_table_exists = mdb_exe(f'''
            SELECT name
            FROM sqlite_master
            WHERE type = "table"
              AND name = "page_hist_{freq}"''').fetchone()

        # recreate history table
        if renew_tables or not hist_table_exists:
            mdb_exe(f'DROP TABLE IF EXISTS page_hist_{freq}')
            mdb_exe(f'''
                CREATE TABLE page_hist_{freq} (
                    timestamp	TEXT NOT NULL,
                    path_id	    INTEGER NOT NULL,
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
                    PRIMARY KEY (timestamp, path_id),
                    FOREIGN KEY (path_id)
                        REFERENCES paths (path_id)
                            ON UPDATE RESTRICT
                            ON DELETE RESTRICT)''')
            latest_history = '19561017-0500'
        else:
            latest_history = mdb_exe(
                f'SELECT max(timestamp) FROM page_hist_{freq}').fetchone()[0]

        # cycle over scrapes
        for timestamp, scr_dir in scrape_dirs(
                master_dir, latest_history, max_timestamp, frequency=freq[0]):
            sdb_file = scr_dir / 'scrape.db'
            mdb_exe(f'ATTACH DATABASE "{str(sdb_file)}" AS scrape')

            # register all paths (effectively adding only the new ones)
            mdb_exe('''
                INSERT OR IGNORE INTO main.paths (path)
                SELECT path
                FROM scrape.pages
                ORDER BY path''')

            # register new pages with life value of 1
            qry = f'''
                WITH
                    scraped_pages_info AS (
                    SELECT path_id, title, description, num_h1s, first_h1, 
                        language, modified, pagetype, classes, ed_text,
                        aut_text, business, category
                    FROM scrape.pages
                    LEFT JOIN scrape.pages_info USING (page_id)
                    LEFT JOIN main.paths USING (path)
                    )
                INSERT INTO main.page_hist_{freq}
                SELECT '{timestamp}' AS timestamp, scr.*, 1 AS life
                FROM scraped_pages_info AS scr
                LEFT JOIN main.page_hist_{freq} AS his USING (path_id)
                WHERE his.path_id IS NULL
                ORDER BY path_id'''
            mdb_exe(qry)

            # negate life value of pages that died
            qry = f'''
                WITH
                    latest_life_values AS (
                        SELECT max(timestamp) AS timestamp, path_id, life
                        FROM main.page_hist_{freq}
                        GROUP BY path_id
                    )
                INSERT INTO main.page_hist_{freq} (timestamp, path_id, life)
                SELECT '{timestamp}', path_id, -life AS life
                FROM latest_life_values
                LEFT JOIN main.paths USING (path_id)
                LEFT JOIN scrape.pages AS scr USING (path)
                WHERE life > 0 AND scr.path IS NULL
                ORDER BY path_id'''
            mdb_exe(qry)

            # get relevant aspects names of a page
            qry = 'SELECT name FROM scrape.pragma_table_info("pages_full")'
            field_names = [row[0] for row in mdb_exe(qry).fetchall()
                           if row[0] not in ('page_id', 'path', 'doc')]

            # register changed aspects of all pages
            # - new pages are registered (with all aspects) already, so no harm
            #   is done to include them
            # the resulting query is formatted with spacing and linebreaks for
            # debugging purposes (do not alter the string literals in this
            # source)
            qry = '''
                WITH
                    latest_hist_aspects AS (
                        SELECT DISTINCT
                            path_id,'''
            for field in (*field_names, 'life'):
                qry += f'''
                            last_value({field}) OVER (
                                PARTITION BY path_id
                                ORDER BY 
                                    (CASE WHEN {field} ISNULL THEN 0 ELSE 1 END),
                                    timestamp
                                ROWS BETWEEN UNBOUNDED PRECEDING
                                    AND UNBOUNDED FOLLOWING
                            ) AS {field},'''
            qry = qry[:-1] + f'''
                        FROM main.page_hist_{freq}
                    ),
                    changed_pages AS (
                        SELECT
                            path_id,'''
            for field in field_names:
                qry += f'''
                            CASE WHEN scr.{field} = his.{field} 
                                 THEN NULL
                                 ELSE scr.{field}
                            END AS {field},'''
            qry += f'''
                            CASE WHEN his.life < 0
                                 THEN -his.life + 1
                                 ELSE NULL
                            END AS life
                        FROM scrape.pages_full AS scr
                        LEFT JOIN main.paths USING (path)
                        LEFT JOIN latest_hist_aspects AS his USING (path_id)
                    )
                INSERT INTO main.page_hist_{freq}
                SELECT '{timestamp}', *
                FROM changed_pages
                WHERE '''
            for field in field_names:
                qry += f'''
                    {field} NOT NULL OR '''
            qry = qry[:-4]

            mdb_exe(qry)

            mdb_exe(f'DETACH DATABASE scrape')
            print(f'{freq.capitalize()} scrape history added for {timestamp}')

    mdb_exe('VACUUM')
    mdb.close()

