#!/usr/bin/env python

""" Run-file to scrape www.belastingdienst.nl (version 3.0).

This module is supposed to be run from the command prompt.
"""

import sys
import argparse
from pathlib import Path
from scraper_lib import SCRAPER_VERSION, scrape_site

parser = argparse.ArgumentParser(
    description='Scrape www.belastingdienst.nl',
    usage='%(prog)s [options] master_dir',
    add_help=False)

parser.add_argument(
    '-h', '--help',
    help="show this help message and exit", action='help')
parser.add_argument(
    'master_dir', help="directory with the master database "
                       "and destination of the scrape")
parser.add_argument(
    '-r', default='/wps/wcm/connect', dest='root',
    help="root path that sets the scope of the scrape (default: %(default)s)")
parser.add_argument(
    '-s', default='start_urls.txt', dest='start_urls',
    help="path to the file with url's to start the scrape "
         "(default: %(default)s)")
parser.add_argument(
    '-n', dest='no_sitemap', action='store_true',
    help="do not use the sitemap to start the scrape (default: %(default)s)")
parser.add_argument(
    '-x', default=15_000, dest='max_urls',
    help="maximum number of url's to request while scraping "
         "(default: %(default)d)")
parser.add_argument(
    '-w', dest='weekly', action='store_true',
    help="this is a weekly scrape (default: %(default)s)")
parser.add_argument(
    '-m', dest='monthly', action='store_true',
    help="this is a monthly scrape (default: %(default)s)")
parser.add_argument(
    '-o', dest='override', action='store_true',
    help="override date checks for weekly and monthly scrape to force going "
         "into history and have a report (default: %(default)s)")
parser.add_argument(
    '-b', dest='basic', action='store_true',
    help="perform a basic scrape only, independent of and unrelated to the "
         "master database (default: %(default)s)")
parser.add_argument(
    '-u', dest='trans', action="count", default=0,
    help="prepare for uu-based transmission: -u scrape database only, "
         "-uu full scrape directory, -uuu zip with all changed files "
         "(default: no preparation)")
parser.add_argument(
    '-v', '--version', help="show version of the scraper and exit",
    action='version', version=f'%(prog)s {SCRAPER_VERSION}')
args = parser.parse_args()

# Do some input checking
master_dir = Path(args.master_dir)
if not (master_dir.exists() and master_dir.is_dir()):
    sys.exit(f'Error: master directory not found ({master_dir})')
start_urls = Path(args.start_urls)
if not (start_urls.exists() and start_urls.is_file()):
    sys.exit(f"Error: file with start url's not found ({start_urls})")
# Check existence of the master database
master_db = master_dir / 'scrape_master.db'
if not (args.basic or master_db.exists()):
    sys.exit(f"Error: master database not found ({master_db})")
# Is max_urls an integer?
try:
    max_urls = int(args.max_urls)
except ValueError:
    sys.exit(f"Error: maximum number of url's not valid ({args.max_urls})")
# Warn when more transmission preparation is asked with a basic scrape
if args.basic and args.trans > 1:
    print('warning: since a basic scrape does not involve the master '
          'database, it will not be prepared for transmission')

print('scrape started')

scrape_site(master_dir, root=args.root, start_urls=start_urls,
            add_sitemap=not args.no_sitemap, max_urls=max_urls,
            weekly=args.weekly, monthly=args.monthly, override=args.override,
            basic=args.basic, trans=args.trans)

print('scrape finished')
