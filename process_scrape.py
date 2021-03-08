import zipfile
from pathlib import Path

from scraper_lib import sync_page_ids, key_figures, dimensions
from scraper_lib import compile_history, report_scrape
from bd_viauu import bintouu, split_uufile

timestamp = '210301-1707'
mst_dir = Path('/home/jos/bdscraper/scrapes_3.0')
scr_dir = mst_dir / timestamp
mst_db = mst_dir / 'scrape_master.db'
scr_db = scr_dir / 'scrape.db'

# sync_page_ids(scr_db, mst_db)
# key_figures(scr_db, mst_db)
# dimensions(scr_db, mst_db)
# compile_history(scr_db, mst_db)
# report_scrape(scr_db, mst_db)

zip_file = mst_dir / (timestamp + '.zip')
zf = zipfile.ZipFile(zip_file, mode='w')
for f in scr_dir.iterdir():
    zf.write(f, timestamp + '/' + f.name)
zf.close()
uu_file = bintouu(zip_file)
zip_file.unlink()
split_uufile(uu_file, max_mb=30)

print('done')
