from bd_viauu import bintouu, split_uufile

# ============================================================================ #
in_file = 'pp382.7z'
part_max_mb = 26
# ============================================================================ #

uufile = in_file + '.uu'
bintouu(in_file, uufile)
split_uufile(uufile, part_max_mb)
