"""Module for test purposes only"""

from bs4 import BeautifulSoup
from scraper_lib import ScrapeDB, links
from scraper_lib import DataSheet

timestamp = '200921-0430'

test_paths = (
    ('homepage', '/nl/home/home'),
    ('dv-filterpagina', '/nl/auto-en-vervoer/auto-en-vervoer'),
    ('dv-content-pagina', '/nl/auto-en-vervoer/content/fijnstoftoeslag-motorrijtuigenbelasting'),
    ('bib-pagina-1', '/bldcontentnl/belastingdienst/prive/relatie_familie_en_gezondheid/relatie/samenwoners/samenwonen'),
    ('bib-pagina-2', '/bldcontentnl/belastingdienst/prive/auto_en_vervoer/belastingen_op_auto_en_motor/')
)

scrape_dir = '/home/jos/bdscraper/scrapes/' + timestamp + ' - bd-scrape'
db_file = scrape_dir + '/scrape.db'
db = ScrapeDB(db_file)

root_url = db.get_par('root_url')

sheet = DataSheet('Cases', ('Case name', 20),
                  ('Link text', 60), ('Link destination', 150))

for case_name, case_path in test_paths:
    page_string = db.get_page(case_path)
    soup = BeautifulSoup(page_string, features='lxml')
    link_list = links(soup, root_url, root_rel=False, excl_hdr_ftr=True,
                      remove_anchor=False)

    for link_text, link_path in link_list:
        sheet.append([case_name, link_text, link_path])

sheet.save('links.xlsx')
