SELECT l.Pagepath, p.page_id, p.path FROM pages_full AS p LEFT JOIN links AS l ON p.path = l.Linkdestination WHERE pagetype = 'bld-wrapper' and language = 'nl';

-- pages that link to dutch wrappers
SELECT pe.page_id, like('/bldcontent%', Pagepath) as bib, like('/nl%', Pagepath) as dv, Pagepath, pe.pagetype, pe.classes, dw.page_id, Linkdestination, Linktext
FROM dutch_wrappers AS dw
	JOIN links AS l ON dw.path = l.Linkdestination
	JOIN pages_extra AS pe ON l.Pagepath = pe.path;

-- links to pdf's from wrapper pages
SELECT page_id, path, Linkdestination FROM dutch_wrappers JOIN links ON path = Pagepath WHERE Linkdestination LIKE '%.pdf';

SELECT pagetype, count(*) FROM pages_extra GROUP BY pagetype;