import binascii
import os.path
import re
from math import ceil


def bintouu(in_file, out_file):
    """UU-encode a file for text-based transmission.

    This is a simplified version of the encode function of the uu module.

    Args:
        in_file (str): filename or path of the file to be encoded
        out_file (str): filename or path of the uu-encoded output file

    Returns:
        None
    """
    with open(in_file, 'rb') as in_file, open(out_file, 'wb') as out_file:
        data = in_file.read(45)
        while len(data) > 0:
            out_file.write(binascii.b2a_uu(data))
            data = in_file.read(45)


def uutobin(in_file, out_file):
    """Decode a uu-encoded file.

    This function reverses the encoding with the bintouu function of this
    module. As such it yields the original file before encoding.
    It is a simplified version of the decode function of the uu module.

    Args:
        in_file (str): filename or path of the file to be decoded
        out_file (str):filename or path of the decoded output file

    Returns:
        None
    """
    with open(in_file, 'rb') as in_file, open(out_file, 'wb') as out_file:
        s = in_file.readline()
        while s:
            out_file.write(binascii.a2b_uu(s))
            s = in_file.readline()


def split_uufile(in_file, max_mb=15):
    """Split a uu-encoded file in parts.

    The names or paths of the part files is <in_file>-<nn>.txt, where <nn>
    is 01 for the first part, 02 for the second, and so on. Implicitly this
    means that these part files are written in the same directory where
    in_file resides.
    Warning: In case the use of this function will result in more then 25
    part files, a confirmation is requested via the console. Please secure
    that this will not happen in unattended use of this function.

    Args:
        in_file (str): filename or path of the file to be split
        max_mb (float): the max size of each part file in MB

    Returns:
        None
    """
    max_size = int(max_mb * 1000 * 1000)
    dir_path, in_name = os.path.split(in_file)
    name_base = os.path.splitext(in_name)[0]
    if not dir_path:
        dir_path = '.'
    total_parts = ceil(os.path.getsize(in_file) / max_size)
    if total_parts > 25:
        while True:
            answer = input(f'Split into {total_parts} parts [y/n]: ')
            if answer in ('y', 'Y'):
                break
            elif answer in ('n', 'N'):
                return
            else:
                continue
    with open(in_file, newline='') as in_file:
        for part_i in range (1, total_parts + 1):
            out_name = name_base + '-%02i.txt' % part_i
            out_file = os.path.join(dir_path, out_name)
            with open(out_file, 'w', newline='') as out_file:
                out_file.write(in_file.read(max_size))


def merge_uufiles(in_file):
    """Merge a set of uu-encoded files.

    This function reverses the function split_uufiles of this module. As such
    it assumes that the names of the part files end with -<nn>.txt, where <nn>
    is the sequence number of a part, starting from 01. The merged result will
    be written in the same directory as the part files and named as in_file
    without -<nn>.txt.

    Args:
        in_file (str): filename or path of the first part file of the set

    Returns:
        None
    """
    dir_path, in_file = os.path.split(in_file)
    if not dir_path:
        dir_path = '.'
    name_base = in_file[:-7]  # remove '-nn.txt' from end
    part_patt = re.compile(name_base + r'-\d{2}\.txt')
    part_files = sorted(
        [f for f in os.listdir(dir_path) if re.fullmatch(part_patt, f)])
    out_file = os.path.join(dir_path, name_base + '.uu')
    with open(out_file, 'x', newline='') as out_file:
        for in_file in part_files:
            in_file = os.path.join(dir_path, in_file)
            with open(in_file, newline='') as in_file:
                out_file.write(in_file.read())
