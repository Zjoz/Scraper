"""Recreate scrape databases after text-based transmission (version 1.0)."""

from pathlib import Path
from bd_viauu import merge_uufiles, uutobin

# ============================================================================ #
min_timestamp = '201012-0000'   # scrapes before are not processed
max_timestamp = '201012-2359'   # scrapes after are not processed
# ============================================================================ #

# master_dir = Path('C:\\Users\\diepj09\\Documents\\scrapes\\')
master_dir = Path('/home/jos/bdscraper/scrapes')

# cycle over all scrape directories
dirs = [d for d in master_dir.glob('??????-???? - bd-scrape') if d.is_dir()]
for scrape_dir in dirs:
    timestamp = scrape_dir.name[:11]
    if timestamp <= min_timestamp or timestamp >= max_timestamp:
        # scrape is not within timestamp range; get next one
        continue
    first_part_file = scrape_dir / 'scrape.db-01.txt'
    if not first_part_file.exists():
        # directory does not contain part files; get next dir
        continue

    uu_file = merge_uufiles(first_part_file, delete=False)
    uutobin(uu_file)
