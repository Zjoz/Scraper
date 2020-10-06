SELECT language, pagetype, count(page_id) as c FROM pages_info GROUP BY language, pagetype ORDER BY language DESC, c DESC

