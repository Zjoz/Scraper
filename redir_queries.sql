-- number alias redirs
SELECT alias_per_url, count(alias_per_url) FROM (SELECT redir_path, count(*) as alias_per_url FROM redirs WHERE type = 'alias' GROUP BY redir_path) GROUP BY alias_per_url;

-- numbers of redirect types
SELECT type, count(*) FROM redirs GROUP BY type;

-- number slash rewrites
SELECT type, count(*) FROM redirs WHERE req_path || '/' = redir_path or req_path = redir_path || '/' GROUP BY type;

-- number temporary redirects
SELECT type, count(*) FROM redirs WHERE type = 'redir 302';

-- permanent redirect but not slash rewrite
SELECT type, count(*) FROM redirs WHERE type = 'redir 301' and not (req_path || '/' = redir_path or req_path = redir_path || '/');
