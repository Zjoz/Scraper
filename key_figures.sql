-- MULTI DIMENSION QUERIES ------------------------------------------------------------------------------------------------------------------------------------------------------

-- # pages per language per pagetype
SELECT language, pagetype, count(page_id) as c FROM pages_full GROUP BY language, pagetype ORDER BY language DESC, c DESC;


-- H1 QUERIES -------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- page count per number of h1's
SELECT num_h1s, count(*) FROM pages_full GROUP BY num_h1s;

-- number of pages with more than one h1
SELECT count(*) FROM pages_full WHERE num_h1s > 1;

-- number of multi-h1's per pagetype
SELECT pagetype, count(*) FROM pages_full WHERE num_h1s > 1 GROUP BY pagetype;

-- number of pages with no h1
SELECT count(*) FROM pages_full WHERE num_h1s = 0;


-- REDIR QUERIES ----------------------------------------------------------------------------------------------------------------------------------------------------------------

-- number alias redirs
SELECT alias_per_url, count(alias_per_url) FROM (SELECT redir_path, count(*) as alias_per_url FROM redirs WHERE type = 'alias' GROUP BY redir_path) GROUP BY alias_per_url;

-- numbers of redirect types
SELECT type, count(*) FROM redirs GROUP BY type;

-- number of real redirects, so without aliases
SELECT count(*) FROM redirs WHERE type != 'alias';

-- number of slash rewrites
SELECT type, count(*) FROM redirs WHERE req_path || '/' = redir_path or req_path = redir_path || '/' GROUP BY type;

-- number of temporary redirects
SELECT type, count(*) FROM redirs WHERE type = 'redir 302';

-- permanent redirect but not slash rewrite
SELECT type, count(*) FROM redirs WHERE type = 'redir 301' and not (req_path || '/' = redir_path or req_path = redir_path || '/');


-- OTHER FIGURES ----------------------------------------------------------------------------------------------------------------------------------------------------------------

-- number of pages without title
SELECT count(*) FROM pages_full WHERE title is NULL;

