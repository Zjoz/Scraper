BEGIN TRANSACTION;
DROP TABLE IF EXISTS "dimensions";
CREATE TABLE IF NOT EXISTS "dimensions" (
	"timestamp"	TEXT NOT NULL,
	"language"	TEXT NOT NULL,
	"business"	TEXT NOT NULL,
	"category"	TEXT NOT NULL,
	"pagetype"	TEXT NOT NULL,
	"num_pages"	INTEGER NOT NULL
);
DROP TABLE IF EXISTS "descriptions";
CREATE TABLE IF NOT EXISTS "descriptions" (
	"name"	TEXT NOT NULL,
	"dutch"	TEXT NOT NULL,
	PRIMARY KEY("name")
);
DROP TABLE IF EXISTS "key_figures";
CREATE TABLE IF NOT EXISTS "key_figures" (
	"timestamp"	TEXT NOT NULL,
	"name"	TEXT NOT NULL,
	"value"	INTEGER NOT NULL,
	PRIMARY KEY("timestamp","name")
);
DROP TABLE IF EXISTS "scranges";
CREATE TABLE IF NOT EXISTS "scranges" (
	"timestamp"	TEXT NOT NULL
);
DROP VIEW IF EXISTS "kf_dutch_desc";
CREATE VIEW "kf_dutch_desc" AS SELECT * FROM key_figures LEFT JOIN descriptions USING (name);
COMMIT;
