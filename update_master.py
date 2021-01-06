"""Update and maintain various scrape info (version 2.0).

This modules is intended to update all tables of the scrape_master database
after new scrapes have been made. The description table is not updated however,
since it should be hand-loaded or manually maintained.
"""

from pathlib import Path
from scraper_lib import master_figures, compile_history

# ============================================================================ #
min_timestamp = '200831-0000'   # scrapes before are not processed
max_timestamp = '210131-2359'   # scrapes after are not processed
within_bd = False               # True when running on the DWB

figures = True                  # update figures in master database

history = True                  # compile history
renew_tables = True             # to refresh complete history
weekly = True                   # compile weekly history
monthly = True                  # compile monthly history
# ============================================================================ #

# establish master scrape directory
if within_bd:
    master_dir = Path('C:/Users', 'diepj09', 'Documents/scrapes')
else:
    master_dir = Path('/home/jos/bdscraper/scrapes')

# (re)write typical figures of scrape range to the master database
if figures:
    master_figures(master_dir, min_timestamp, max_timestamp)

# compile history of scrape range into the master database
if history:
    compile_history(master_dir, max_timestamp,
                    weekly, monthly, renew_tables)
