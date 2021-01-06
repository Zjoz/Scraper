"""Generate workbook reports for a range of scrapes (version 1.0)."""

import difflib
import sqlite3
import xlsxwriter
import re
from xlsxwriter.utility import xl_range_abs
from pathlib import Path
from operator import itemgetter

from scraper_lib import scrape_dirs

# ============================================================================ #
min_timestamp = '201214-0000'   # scrapes before are not processed
max_timestamp = '201214-2359'   # scrapes after are not processed
weekly = True                   # compile weekly history
monthly = False                 # compile monthly history
full_info = True                # add sheets with pages, links, redirs and paths
within_bd = False               # True when running on the DWB
# ============================================================================ #


def mod_factor(ref_text, act_text):
    """Calculate the modification factor of two texts.

    The returned value is an (arbitrary) measure of the difference between
    two texts on a scale from 0 (textst are exactly equal) to 1 (textst are
    completely different). The value is calculated as 1 - (SR1 + SR2)/2,
    where SR stands for the similarity ratio as defined in the Python
    standard difflib module. SR1 represents the similarity of both texts. SR2
    is the similarity of the sorted set of words from both texts. Averaging
    these ratios has the effect that changes in both wording and phrasing are
    distinguished from changes in pharasing or wording only.

    Args:
        ref_text (str): text acting as reference
        act_text (str): actual text to compare against the reference

    Returns:
        float in the range of 0 to 1
    """
    sm = difflib.SequenceMatcher(a=ref_text, b=act_text)
    lib_ratio = sm.ratio()
    sm.set_seq1(a=sorted(list(set(ref_text.split()))))
    sm.set_seq2(b=sorted(list(set(act_text.split()))))
    set_ratio = sm.ratio()
    return 1 - (lib_ratio + set_ratio) / 2


def shade(row_nr, total_rows):
    """Criterium for shading a row.
    
    Depending on the total number of rows the shading will change after one,
    two or three rows.
    
    Args:
        row_nr (int): actual row
        total_rows (int): total number of rows

    Returns:
        bool: to shade or not to shade
    """
    if total_rows <= 6:
        heigth = 1
    elif total_rows < 12:
        heigth = 2
    else:
        heigth = 3
    return row_nr % (2 * heigth) not in list(range(1, heigth + 1))


def dict_union(*args):
    """Return union of the all dicts.

    Keys that are common in d's will contain values from last d.

    Args:
        args (dict):

    Returns:
        dict:
    """
    result = None
    for d in args:
        if result:
            result.update(d)
        else:
            result = d.copy()
    return result


# establish scrape directories
if within_bd:
    master_dir = Path('C:/Users', 'diepj09', 'Documents/scrapes')
else:
    master_dir = Path('/home/jos/bdscraper/scrapes')

# connect master database
mdb_file = master_dir / 'scrape_master.db'
mdb = sqlite3.connect(mdb_file, isolation_level=None)
mdb_exe = mdb.execute

for do, freq in [(weekly, 'weekly'), (monthly, 'monthly')]:
    if not do:
        continue

    for timestamp, scr_dir in scrape_dirs(
            master_dir, min_timestamp, max_timestamp, frequency=freq[0]):

        # get timestamp of previous scrape with same periodicity
        qry = f'''
            SELECT max(timestamp)
            FROM scrapes
            WHERE periodicity = '{freq[0]}'
                AND timestamp < '{timestamp}' '''
        prev_timestamp = mdb_exe(qry).fetchone()[0]

        # attach scrape database
        sdb_file = scr_dir / 'scrape.db'
        mdb_exe(f'ATTACH DATABASE "{str(sdb_file)}" AS scrape')

        # initiate report workbook
        xlsx_file = scr_dir / f'{timestamp} - {freq} report.xlsx'
        wb = xlsxwriter.Workbook(xlsx_file, {'constant_memory': True})

        # set formats for the various sheets
        border_color = '#A9A9A9'
        shade_color = '#E8E8E8'
        hdr = {'bold': True, 'font_color': '#FFFFFF', 'fg_color': '#808080',
               'border_color': '#FFFFFF', 'left': 1, 'right': 1}
        val = {'border_color': border_color, 'left': 1,  'right': 1}
        ctr = {'align': 'center'}
        shd = {'fg_color': shade_color}
        delta = {'num_format': '+#;-#;-'}
        dec2 = {'num_format': '0.00'}
        fmt_hdr = wb.add_format(hdr)
        fmt_val = wb.add_format(val)
        fmt_val_shd = wb.add_format(dict_union(val, shd))
        fmt_val_ctr = wb.add_format(dict_union(val, ctr))
        fmt_val_ctr_shd = wb.add_format(dict_union(val, ctr, shd))
        fmt_val_delta = wb.add_format(dict_union(val, ctr, delta))
        fmt_val_delta_shd = wb.add_format(dict_union(val, ctr, delta, shd))
        fmt_val_dec2 = wb.add_format(dict_union(val, ctr, dec2))
        fmt_val_dec2_shd = wb.add_format(dict_union(val, ctr, dec2, shd))

        # add and fill a sheet with scrape parameters
        ws = wb.add_worksheet('Parameters')
        # bug: hide_gridlines(2) on first sheet will hide them for all sheets
        ws.hide_gridlines(0)
        col_spec = [('Name', 11), ('Value', 45)]
        for col in range(len(col_spec)):
            ws.write(0, col, col_spec[col][0], fmt_hdr)
            ws.set_column(col, col, col_spec[col][1])
        qry = 'SELECT * FROM scrape.parameters'
        row, col = 0, -1
        qry_results = mdb_exe(qry).fetchall()
        rows = len(qry_results)
        for qry_result in qry_results:
            row += 1
            col = -1
            fmt = fmt_val_shd if shade(row, rows) else fmt_val
            for field in qry_result:
                col += 1
                ws.write(row, col, field, fmt)

        # add and fill a sheet with key figures
        ws = wb.add_worksheet('Key figures')
        ws.hide_gridlines(0)
        col_spec = [('Description', 75), ('Name', 26), ('Value', 9)]
        if prev_timestamp:
            col_spec.append((f'Versus prev. {freq.split("ly")[0]}', 20))
        for col in range(len(col_spec)):
            ws.write(0, col, col_spec[col][0], fmt_hdr)
            ws.set_column(col, col, col_spec[col][1])
        ws.freeze_panes('A2')
        if prev_timestamp:
            qry = f'''
                WITH
                    names AS (
                        SELECT DISTINCT name
                        FROM key_figures
                        WHERE timestamp = '{prev_timestamp}'
                            OR timestamp = '{timestamp}'
                        ORDER BY ROWID
                    ),
                    prev (name, pv) AS (
                        SELECT name, value
                        FROM key_figures
                        WHERE timestamp = '{prev_timestamp}'
                    ),
                    cur (name, cv) AS (
                        SELECT name, value
                        FROM key_figures
                        WHERE timestamp = '{timestamp}'
                    ),
                    vals (name, pv, cv) AS (
                        SELECT
                            name,
                            CASE WHEN pv ISNULL THEN 0 ELSE pv END,
                            CASE WHEN cv ISNULL THEN 0 ELSE cv END
                        FROM names
                        LEFT JOIN prev USING (name)
                        LEFT JOIN cur USING (name)
                    )
                SELECT english, name, cv, cv - pv
                FROM vals
                LEFT JOIN descriptions USING (name)'''
        else:
            qry = f'''
                SELECT english, name, value
                FROM key_figures
                LEFT JOIN descriptions USING (name)
                WHERE timestamp = '{timestamp}' '''
        row, col = 0, -1
        shaded = True
        last_group = ''
        for qry_result in mdb_exe(qry).fetchall():
            row += 1
            col = -1
            name_parts = qry_result[1].split('_')
            group = name_parts[0]
            if group == 'pages' and len(name_parts) > 1:
                group += '_' + name_parts[1]
            if group != last_group:
                shaded = not shaded
                last_group = group
            fmt = fmt_val_shd if shaded else fmt_val
            fmt_delta = fmt_val_delta_shd if shaded else fmt_val_delta
            for field in qry_result:
                col += 1
                if col == 3:
                    ws.write(row, col, field, fmt_delta)
                else:
                    ws.write(row, col, field, fmt)
            ws.autofilter(0, 0, row, col)

        if prev_timestamp:

            # add and fill a sheet with removed pages
            ws = wb.add_worksheet('Removed pages')
            ws.hide_gridlines(0)
            col_spec = [
                ('path_id', 10), ('Path', 30), ('Title', 30),
                ('Description', 30), ('First h1', 30), ("# h1's", 8),
                ('Language', 11), ('Modified', 14), ('Page type', 15),
                ('Classes', 25), ('Business', 12), ('Category', 11),
                ('Editorial text', 55), ('Automated text', 55)]
            for col in range(len(col_spec)):
                ws.write(0, col, col_spec[col][0], fmt_hdr)
                ws.set_column(col, col, col_spec[col][1])
            ws.freeze_panes('C2')
            pdb_filename = str(sdb_file).replace(timestamp, prev_timestamp)
            mdb_exe(f'ATTACH DATABASE "{pdb_filename}" AS prev_scrape')
            qry = f'''
                WITH
                    removed_paths AS (
                        SELECT path_id
                    FROM page_hist_{freq}
                    WHERE timestamp = '{timestamp}'
                        AND life < 0
                    )
                SELECT path_id, path, title, description, first_h1, num_h1s, language,
                    modified, pagetype, classes, business, category, ed_text, aut_text
                FROM prev_scrape.pages_full
                LEFT JOIN paths USING (path)
                WHERE path_id IN removed_paths
                ORDER BY path_id'''
            row, col = 0, -1
            for qry_result in mdb_exe(qry).fetchall():
                row += 1
                col = -1
                for field in qry_result:
                    col += 1
                    if col in (0, 5, 6, 7, 11):
                        ws.write(row, col, field, fmt_val_ctr)
                    else:
                        ws.write(row, col, field, fmt_val)
                ws.autofilter(0, 0, row, col)
            mdb_exe('DETACH prev_scrape')

            # add and fill a sheet with new pages
            ws = wb.add_worksheet('New pages')
            ws.hide_gridlines(0)
            col_spec = [
                ('path_id', 10), ('Path', 30), ('Title', 30),
                ('Description', 30), ('First h1', 30), ("# h1's", 8),
                ('Language', 11), ('Modified', 14), ('Page type', 15),
                ('Classes', 25), ('Business', 12), ('Category', 11),
                ('Editorial text', 55), ('Automated text', 55)]
            for col in range(len(col_spec)):
                ws.write(0, col, col_spec[col][0], fmt_hdr)
                ws.set_column(col, col, col_spec[col][1])
            ws.freeze_panes('C2')
            qry = f'''
                SELECT path_id, path, title, description, first_h1, num_h1s,
                    language, modified, pagetype, classes, business, category,
                    ed_text, aut_text
                FROM page_hist_{freq}
                LEFT JOIN paths USING (path_id)
                WHERE timestamp = '{timestamp}'
                    AND life = 1
                ORDER BY path_id'''
            row, col = 0, -1
            for qry_result in mdb_exe(qry).fetchall():
                row += 1
                col = -1
                for field in qry_result:
                    col += 1
                    if col in (0, 5, 6, 7, 11):
                        ws.write(row, col, field, fmt_val_ctr)
                    else:
                        ws.write(row, col, field, fmt_val)
                ws.autofilter(0, 0, row, col)

            # add and fill a sheet detailing the aspects of the changed pages
            ws = wb.add_worksheet('Changed aspects')
            ws.hide_gridlines(2)
            col_spec = [
                ('path_id', 10), ('Path', 30), ('Business', 12),
                ('Language', 11), ('Pagetype', 15), ('Aspect', 13),
                ('Current value', 50), ('Modification factor', 20),
                ('Previous value', 50), ('Timestamp previous value', 26)]
            for col in range(len(col_spec)):
                ws.set_column(col, col, col_spec[col][1])
                ws.write(0, col, col_spec[col][0], fmt_hdr)
            ws.freeze_panes('A2')
            # get relevant aspect names of a page
            qry = f'SELECT name FROM pragma_table_info("page_hist_{freq}")'
            aspects = [row[0] for row in mdb_exe(qry).fetchall()
                       if row[0] not in ('timestamp', 'path_id', 'life')]
            qry_results = []
            for aspect in aspects:
                # query to get current and previous values of changed aspect
                qry = f'''
                    SELECT
                        path_id, path, scr.business, scr.language, scr.pagetype,
                        '{aspect}' AS aspect,
                        lch.{aspect} AS val_new,
                        his.{aspect} AS val_old,
                        max(his.timestamp) AS ts_old
                    FROM page_hist_{freq} AS lch
                    JOIN page_hist_{freq} AS his USING (path_id)
                    LEFT JOIN paths USING (path_id)
                    LEFT JOIN scrape.pages_full AS scr USING (path)
                    WHERE lch.timestamp = '{timestamp}'
                        AND (lch.life IS NULL OR lch.life > 1)
                        AND lch.{aspect} NOT NULL
                        AND his.{aspect} NOT NULL
                        AND his.timestamp < '{timestamp}'
                    GROUP BY path_id'''
                for qry_result in mdb_exe(qry).fetchall():
                    if aspect in ('title', 'description', 'first_h1',
                                  'ed_text', 'aut_text'):
                        new_txt, old_txt = qry_result[6:8]
                        mf = mod_factor(old_txt, new_txt)
                    else:
                        mf = None
                    qry_result = list(qry_result)
                    qry_result.insert(7, mf)
                    qry_results.append(qry_result)
            # sort on page_id and aspect
            qry_results.sort(key=itemgetter(0, 5))
            # write to sheet
            row, col = 0, -1
            rows = len(qry_results)
            shaded = True
            last_id = 0
            for qry_result in qry_results:
                row += 1
                col = -1
                if last_id != qry_result[0]:
                    shaded = not shaded
                    last_id = qry_result[0]
                fmt = fmt_val_shd if shaded else fmt_val
                fmt_ctr = fmt_val_ctr_shd if shaded else fmt_val_ctr
                fmt_2dec = fmt_val_dec2_shd if shaded else fmt_val_dec2
                for field in qry_result:
                    col += 1
                    if col in (0, 3, 9):
                        ws.write(row, col, field, fmt_ctr)
                    elif col == 7:
                        ws.write(row, col, field, fmt_2dec)
                    else:
                        ws.write(row, col, field, fmt)
            ws.autofilter(0, 0, row, col)

        if full_info:

            # add and fill sheet with all pages of the scrape
            ws = wb.add_worksheet('All pages')
            ws.hide_gridlines(0)
            col_spec = [
                ('path_id', 10), ('Path', 30), ('Title', 30),
                ('Description', 30), ('First h1', 30), ("# h1's", 8),
                ('Language', 11), ('Modified', 14), ('Page type', 15),
                ('Classes', 25), ('Business', 12), ('Category', 11),
                ('Editorial text', 55), ('Editorial words', 16),
                ('Automated text', 55)]
            for col in range(len(col_spec)):
                ws.write(0, col, col_spec[col][0], fmt_hdr)
                ws.set_column(col, col, col_spec[col][1])
            ws.freeze_panes('C2')
            qry = '''
                SELECT path_id, path, title, description, first_h1, num_h1s,
                    language, modified, pagetype, classes, business, category, 
                    ed_text, aut_text
                FROM scrape.pages_full
                LEFT JOIN main.paths USING (path)
                ORDER BY path_id'''
            row, col = 0, -1
            for qry_result in mdb_exe(qry).fetchall():
                ed_text = qry_result[12]
                wrd_cnt = len(re.findall(r'\w+', ed_text))
                qry_result = list(qry_result)
                qry_result.insert(13, wrd_cnt)
                row += 1
                col = -1
                for field in qry_result:
                    col += 1
                    if col in (0, 5, 6, 7, 11, 13):
                        ws.write(row, col, field, fmt_val_ctr)
                    else:
                        ws.write(row, col, field, fmt_val)
                ws.autofilter(0, 0, row, col)

            # add and fill sheet with all editorial links of the scrape
            ws = wb.add_worksheet('Editorial links')
            ws.hide_gridlines(2)
            col_spec = [
                ('page_path_id', 15), ('Page path', 50), ('Link text', 50),
                ('link_path_id', 15), ('Link path', 50), ('Link URL', 50)]
            for col in range(len(col_spec)):
                ws.write(0, col, col_spec[col][0], fmt_hdr)
                ws.set_column(col, col, col_spec[col][1])
            ws.freeze_panes('A2')
            qry = '''
                SELECT pp.path_id, page_path, link_text,
                    pl.path_id, link_path, ext_url 
                FROM scrape.ed_links_expl AS scr
                LEFT JOIN paths AS pp ON scr.page_path = pp.path
                LEFT JOIN paths AS pl ON scr.link_path = pl.path
                ORDER BY pp.path_id'''
            row, col = 0, -1
            shaded = True
            last_id = 0
            qry_results = mdb_exe(qry).fetchall()
            rows = len(qry_results)
            for qry_result in qry_results:
                row += 1
                col = -1
                if last_id != qry_result[0]:
                    shaded = not shaded
                    last_id = qry_result[0]
                fmt = fmt_val_shd if shaded else fmt_val
                fmt_ctr = fmt_val_ctr_shd if shaded else fmt_val_ctr
                for field in qry_result:
                    col += 1
                    if col in (0, 3):
                        ws.write(row, col, field, fmt_ctr)
                    else:
                        ws.write(row, col, field, fmt)
                ws.autofilter(0, 0, row, col)

            # add and fill sheet with all editorial links of the scrape
            ws = wb.add_worksheet('Redirects and aliases')
            ws.hide_gridlines(0)
            col_spec = [
                ('Type', 8), ('req_path_id', 15), ('Requested path', 100),
                ('red_path_id', 15), ('Redirected path', 100)]
            for col in range(len(col_spec)):
                ws.write(0, col, col_spec[col][0], fmt_hdr)
                ws.set_column(col, col, col_spec[col][1])
            ws.freeze_panes('A2')
            qry = '''
                SELECT type, req.path_id, req_path, red.path_id, redir_path 
                FROM scrape.redirs AS scr
                LEFT JOIN main.paths AS req ON scr.req_path = req.path
                LEFT JOIN main.paths AS red ON scr.redir_path = red.path
                ORDER BY req_path'''
            row, col = 0, -1
            for qry_result in mdb_exe(qry).fetchall():
                row += 1
                col = -1
                for field in qry_result:
                    col += 1
                    if col in (0, 1, 3):
                        ws.write(row, col, field, fmt_val_ctr)
                    else:
                        ws.write(row, col, field, fmt_val)
                ws.autofilter(0, 0, row, col)

            # add and fill a sheet with all relevant paths for this scrape
            ws = wb.add_worksheet('Paths')
            ws.hide_gridlines(2)
            col_spec = [('path_id', 10), ('Path', 200)]
            for col in range(len(col_spec)):
                ws.write(0, col, col_spec[col][0], fmt_hdr)
                ws.set_column(col, col, col_spec[col][1])
            ws.freeze_panes('A2')
            qry = f'''
                SELECT path_id, path
                FROM scrape.pages
                LEFT JOIN main.paths USING (path)
                UNION
                SELECT DISTINCT path_id, path
                FROM main.page_hist_{freq}
                LEFT JOIN main.paths USING (path_id)
                WHERE timestamp = '{timestamp}'
                ORDER BY path_id'''
            row, col = 0, -1
            for qry_result in mdb_exe(qry).fetchall():
                row += 1
                col = -1
                for field in qry_result:
                    col += 1
                    if col == 0:
                        ws.write(row, col, field, fmt_val_ctr)
                    else:
                        ws.write(row, col, field, fmt_val)
                ws.autofilter(0, 0, row, col)
            wb.define_name('paths', f'=Paths!{xl_range_abs(1, 0, row, col)}')

        mdb_exe('DETACH scrape')
        wb.close()
        print(f'{freq.capitalize()} scrape report generated for {timestamp}')

mdb.close()
