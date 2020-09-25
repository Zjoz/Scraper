"""Module for test and exploring (version 0.2)"""


import os
from datetime import date
from bs4 import BeautifulSoup
from scraper_lib import ScrapeDB, DataSheet
from scraper_lib import title, language, mod_date, page_type, classes, h1s

# ============================================================================ #
timestamp = '200924-1711'   # determines the scrape that is used
sheets = False              # True will create extraction to spreadsheets
database = True             # True will add/update extraction tables
within_bd = False           # True when running on the DWB
# ============================================================================ #

test_paths = (
    ('homepage', '/nl/home/home'),
    ('dv-filterpagina', '/nl/auto-en-vervoer/auto-en-vervoer'),
    ('dv-content-pagina', '/nl/auto-en-vervoer/content/fijnstoftoeslag-motorrijtuigenbelasting'),
    ('bib-pagina-1', '/bldcontentnl/belastingdienst/prive/relatie_familie_en_gezondheid/relatie/samenwoners/samenwonen'),
    ('bib-pagina-2', '/bldcontentnl/belastingdienst/prive/auto_en_vervoer/belastingen_op_auto_en_motor/'),
    ('themapagina', '/bldcontentnl/belastingdienst/douane_voor_bedrijven/uitvoer/'),
    ('clusterpagina', '/bldcontentnl/belastingdienst/prive/inkomstenbelasting/hoe_werkt_inkomstenbelasting/hoe_werkt_inkomstenbelasting'),
    ('wegwijzerpagina', '/bldcontentnl/belastingdienst/prive/internationaal/aangifte_inkomstenbelasting/oudere-aangiften-buitenlandse-belastingplicht/oudere-aangiften-buitenlandse-belastingplicht'),
    ('doelpagina', '/bldcontentnl/belastingdienst/prive/douane/reisbagage/douanecontroles/100-procent-controles')
)

# establish directory of the scrape
if within_bd:
    scrapes_dir = 'C:\\Users\\diepj09\\Documents\\scrapes\\'
else:
    scrapes_dir = '/home/jos/bdscraper/scrapes/'
scrape_dir = scrapes_dir + timestamp + ' - bd-scrape'

# connect to the scrape database
db_file = os.path.join(scrape_dir, 'scrape.db')
db = ScrapeDB(db_file, create=False)

# get some parameters from the scrape
root_url = db.get_par('root_url')

if database:
    db.new_attribs()

if sheets:
    sheet = DataSheet(
        'Info',
        ('path', 100),
        ('title', 60),
        ('language', 12),       # new
        ('date modified', 17),  # new
        ('page type', 15),      # new
        ("num h1's", 12),
        ('page classes', 30)    # new
    )

page_num = 0

# for case, path in test_paths:
#     page_string = db.get_page(path)

for page_id, path, page_string in db.pages():

    page_num += 1
    soup = BeautifulSoup(page_string, features='lxml')

    ttl = title(soup, path)
    lang = language(soup, path)
    md = mod_date(soup, path)
    pt = page_type(soup, path)
    h1_list = h1s(soup, path)
    if h1_list:
        num_h1s = len(h1_list)
        h1 = h1_list[0]
    else:
        num_h1s = 0
        h1 = None

    if cl := classes(soup, path):
        cl = ' '.join(map(str, cl))

    if database:
        db.add_attribs(path, ttl, num_h1s, h1, lang, md, pt, cl)

    if sheets:
        sheet.append([path, ttl, lang, md, pt, num_h1s, cl])

    if page_num % 250 == 0:
        print(page_num)

if sheets:
    sheet.save('info.xlsx')
