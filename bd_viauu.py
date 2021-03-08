"""Library to prepare files for text-based transmission (version 1.1)."""

import binascii
from pathlib import Path
from math import ceil


def bintouu(bin_file):
    """UU-encode a file.

    This is a simplified version of the encode function of the public uu module.

    The encoded file is written to the directory of bin_file, with '.uu'
    appended to the name.

    Args:
        bin_file (Path): path of the file to be encoded

    Returns:
        Path: path of the uu-encoded file
    """
    uu_file = bin_file.with_suffix(bin_file.suffix + '.uu')
    with bin_file.open('rb') as in_file, uu_file.open('wb') as out_file:
        data = in_file.read(45)
        while len(data) > 0:
            out_file.write(binascii.b2a_uu(data))
            data = in_file.read(45)
    return uu_file


def uutobin(uu_file, delete=True):
    """Decode a uu-encoded file.

    This function reverses the encoding with the bintouu function of this
    module. As such it yields the original file before encoding.
    It is a simplified version of the decode function of the public uu module.

    The decoded file is written to the directory of the uu_file, without the
    '.uu' suffix.

    Args:
        uu_file (Path): path of the file to be decoded
        delete (bool): if true, delete the uu-encoded file after use

    Returns:
        Path: path of the decoded file
    """
    bin_file = uu_file.with_suffix('')
    with uu_file.open('rb') as in_file, bin_file.open('wb') as out_file:
        s = in_file.readline()
        while s:
            out_file.write(binascii.a2b_uu(s))
            s = in_file.readline()
    if delete:
        uu_file.unlink()
    return bin_file


def split_uufile(uu_file, max_mb=15, delete=True):
    """Split a uu-encoded file in parts.

    The part files are written to the same directory as the uu-encoded file.
    The names of these part files have the next format: <uu_file>-<nn>.txt,
    where <nn> is 01 for the first part, 02 for the second, and so on.

    Warning: In case the use of this function will result in more then 25
    part files, a confirmation is requested via the console. Please ensure
    that this will not happen in unattended use of this function.

    Args:
        uu_file (Path): path of the uu-encoded file to be split
        max_mb (float): max size of each part file in MB
        delete (bool): if true, delete the uu-encodede files after use

    Returns:
        Path: path of the last part file
    """
    max_size = int(max_mb * 1000 * 1000)
    total_parts = ceil(uu_file.stat().st_size / max_size)
    if total_parts > 25:
        while True:
            answer = input(f'Split into {total_parts} parts [y/n]: ')
            if answer in ('y', 'Y'):
                break
            elif answer in ('n', 'N'):
                return
            else:
                continue
    with uu_file.open(newline='') as in_file:
        for p in range (1, total_parts + 1):
            part_name = f'{uu_file.stem}-{p:02}.txt'
            part_file = uu_file.with_name(part_name)
            with part_file.open('w', newline='') as out_file:
                out_file.write(in_file.read(max_size))
    if delete:
        uu_file.unlink()
    return part_file


def merge_uufiles(part_file, delete=True):
    """Merge a set of uu-encoded files.

    This function reverses the effect of the split_uufiles function of this
    module. As such it assumes that the names of the part files end with
    -<nn>.txt, where <nn> is the sequence number of a part. The merged result
    will be written in the same directory as the part files and named as
    in_file without -<nn>.txt.

    Args:
        part_file (Path): path of one of the part files
        delete (bool): if true, delete the part files after use

    Returns:
        Path: path of the uu-encoded file
    """
    name_base = part_file.name[:-7]  # remove '-nn.txt' from end
    all_part_files = sorted(part_file.parent.glob('*-??.txt'))
    uu_file = part_file.with_name(name_base + '.uu')
    with open(uu_file, 'w', newline='') as out_file:
        for part_file in all_part_files:
            with part_file.open(newline='') as in_file:
                out_file.write(in_file.read())
            if delete:
                part_file.unlink()
    return uu_file
