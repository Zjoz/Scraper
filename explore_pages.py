"""Module for test and exploring (version 0.4)"""

from pathlib import Path
from bs4 import BeautifulSoup
from scraper_lib import ScrapeDB, DataSheet

# ============================================================================ #
timestamp = '201019-0300'   # determines the scrape that is used
within_bd = False           # True when running on the DWB
# ============================================================================ #

test_paths = (
    ('homepage', '/nl/home/home'),
    ('bld-filter-1', '/nl/auto-en-vervoer/auto-en-vervoer'),
    ('bld-filter-2', '/nl/intermediairs/intermediairs'),
    ('bld-dv-content-1', '/nl/auto-en-vervoer/content/fijnstoftoeslag-motorrijtuigenbelasting'),
    ('bld-dv-content-2', '/nl/voorlopige-aanslag/content/hoe-weet-ik-of-ik-geld-terugkrijg-of-moet-betalen'),
    ('bld-targetGroup-1', '/bldcontentde/belastingdienst/unternehmen/unternehmen'),
    ('bld-tragetGroup-2', '/bldcontenten/belastingdienst/business/business'),
    ('bld-landing-1', '/bldcontentnl/campagnes/landingspaginas/prive/educatie/educatie'),
    ('bld-landing-2', '/bldcontentnl/campagnes/landingspaginas/zakelijk/sport/sport'),
    ('bld-bd-1', '/bldcontentnl/berichten/belangrijke_datums/uitbetaling-toeslag-januari-2021'),
    ('bld-bd-2', '/bldcontentnl/berichten/belangrijke_datums/uitbetaling-voorlopige-aanslag-december-2020'),
    ('bld-sitemap-1', '/bldcontenten/belastingdienst/customs/tariff/tariff'),
    ('bld-sitemap-2', '/bldcontentnl/belastingdienst/zakelijk/aangifte_betalen_en_toezicht/aangifte_betalen_en_toezicht'),
    ('bld-overview-1', '/bldcontentde/themaoverstijgend/broschuren_und_veroffentlichung/broschuren_und_veroffentlichung'),
    ('bld-overview-2', '/bldcontentnl/themaoverstijgend/programmas_en_formulieren/programmas_en_formulieren_ondernemer'),
    ('bld-cluster-1', '/bldcontentnl/belastingdienst/prive/werk_en_inkomen/werken/werken'),
    ('bld-cluster-2', '/bldcontentnl/belastingdienst/zakelijk/bijzondere_regelingen/sport_en_belasting/sport_en_belasting'),
    ('bld-direction-1', '/bldcontentnl/belastingdienst/prive/werk_en_inkomen/pensioen_en_andere_uitkeringen/u_hebt_de_aow_leeftijd_bereikt/u_hebt_de_aow_leeftijd_bereikt'),
    ('bld-direction-2', '/bldcontentnl/belastingdienst/zakelijk/aangifte_betalen_en_toezicht/toezicht/handhaving_en_controle/handhaving'),
    ('bld-target-1', '/bldcontentnl/belastingdienst/prive/relatie_familie_en_gezondheid/relatie/overleden/overledene_buiten_nederland'),
    ('bld-target-2', '/bldcontentnl/belastingdienst/prive/auto_en_vervoer/belastingen_op_auto_en_motor/bpm/aangifte_bpm_doen/aangifte_bpm_doen'),
    ('bld-outage-1', '/bldcontentnl/berichten/verstoringen/dubbele-schulden-op-schuldenoverzicht'),
    ('bld-outage-2', '/bldcontentnl/berichten/verstoringen/mogelijk-te-hoge-verminderingen-omzetbelasting'),
    ('bld-newsitem-1', '/bldcontenten/themaoverstijgend/news/export-authorisation-application-form-personal-protective-equipment-available'),
    ('bld-newsitem-2', '/bldcontentnl/berichten/nieuws/u-kunt-nu-voor-meer-belastingen-online-bijzonder-uitstel-van-betaling-aanvragen'),
    ('bld-wrapper-1', '/bldcontentnl/berichten/nieuws/u-kunt-nu-voor-meer-belastingen-online-bijzonder-uitstel-van-betaling-aanvragen'),
    ('bld-wrapper-2', '/bldcontentnl/themaoverstijgend/brochures_en_publicaties/cao_beoordelingen/cao-beoordeling-jeugdzorg-2017-2019')
)

# establish master scrape directory
if within_bd:
    master_dir = Path('C:/Users', 'diepj09', 'Documents/scrapes')
else:
    master_dir = Path('/home/jos/bdscraper/scrapes')
db_file = master_dir / f'{timestamp} - bd-scrape' / 'scrape.db'

# connect to the scrape database
db = ScrapeDB(db_file, create=False)

# get some parameters from the scrape
root_url = db.get_par('root_url')

for case, path in test_paths:

    info = db.page_full_info(path)
    soup = BeautifulSoup(info['doc'], features='lxml')
    description = soup.head.find(attrs={'name': 'description'})
    if description:
        description = description['content']
    else:
        description = None
    url = root_url + path
    print(case, url)
    print(description)
    print()
