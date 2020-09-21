from bd_viauu import merge_uufiles, uutobin

# ============================================================================ #
timestamp = '200914-0500'
# ============================================================================ #

scrapes_dir = 'C:\\Users\\diepj09\\Documents\\scrapes\\'
in_file = scrapes_dir + timestamp + ' - bd-scrape\\scrape.db'
uufile = in_file + '.uu'

merge_uufiles(in_file + '-01.txt')
uutobin(uufile, in_file)