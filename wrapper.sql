SELECT l.page_id, link_id, path, pagetype, classes FROM links AS l
	JOIN pages_full AS p USING (page_id)
WHERE link_id = 96