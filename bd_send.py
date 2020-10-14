from bd_viauu import bintouu, split_uufile

# ============================================================================ #
in_file = '/home/jos/bdscraper/scrapes/201005-0430 - bd-scrape/scrape.db'
part_max_mb = 30
# ============================================================================ #

uufile = in_file + '.uu'
bintouu(in_file, uufile)
split_uufile(uufile, part_max_mb)
