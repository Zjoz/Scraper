"""Module for test and exploring (version 0.2)"""


import os
from datetime import date
from bs4 import BeautifulSoup
from scraper_lib import ScrapeDB, DataSheet

# ============================================================================ #
timestamp = '200926-0937'   # determines the scrape that is used
within_bd = False           # True when running on the DWB
# ============================================================================ #

test_paths = (
    ('homepage', '/nl/home/home'),
    ('dv-filterpagina', '/nl/auto-en-vervoer/auto-en-vervoer'),
    ('dv-content-pagina-1', '/nl/auto-en-vervoer/content/fijnstoftoeslag-motorrijtuigenbelasting'),
    ('dv-content-pagina-2', '/nl/voorlopige-aanslag/content/hoe-weet-ik-of-ik-geld-terugkrijg-of-moet-betalen'),
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

sheet = DataSheet(
    'Info',
    ('path', 100),
    ('title', 60),
    ('language', 12),
    ('date modified', 17),
    ('page type', 15),
    ("num h1's", 12),
    ('page classes', 30)
)

page_num = 0

for case, path in test_paths:
    info = db.get_page_info(path)

# for info in db.pages_full():
#     path = info['path']

    page_num += 1
    soup = BeautifulSoup(info['doc'], features='lxml')

    ttl = info['title']
    lang = info['language']
    md = info['modified']
    pt = info['pagetype']
    num_h1s = info['num_h1s']
    cl = info['classes']
    h1 = info['first_h1']

    # TODO: classes is a list of the strings;
    #           but one of them is 'cluster pagina'!

    sheet.append([path, ttl, lang, md, pt, num_h1s, cl])

    if page_num % 250 == 0:
        print(page_num)

sheet.save('info.xlsx')
