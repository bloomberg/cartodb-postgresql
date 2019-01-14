SET client_min_messages TO error;
\set VERBOSITY default

\i test/overviews/fixtures.sql

SELECT _CDB_Aggregable_Attributes_Expression('base_bare_t'::regclass);
SELECT _CDB_Aggregated_Attributes_Expression('base_bare_t'::regclass);
SELECT _CDB_Aggregated_Attributes_Expression('base_bare_t'::regclass, 'tab');

SELECT CDB_CreateOverviews('base_bare_t'::regclass);
SELECT count(*) FROM _vovw_2_base_bare_t;


SELECT _CDB_Aggregable_Attributes_Expression('base_t'::regclass);
SELECT _CDB_Aggregated_Attributes_Expression('base_t'::regclass);
SELECT _CDB_Aggregated_Attributes_Expression('base_t'::regclass, 'tab');

SELECT CDB_CreateOverviews('base_t'::regclass);
SELECT count(*) FROM _vovw_2_base_t;

SELECT CDB_CreateOverviews('polyg_t'::regclass);

SELECT CDB_CreateOverviews('column_types_t'::regclass);

SELECT CDB_Overviews('base_t'::regclass);
SELECT CDB_Overviews('"public"."base_t"'::regclass);
SELECT CDB_Overviews(ARRAY['base_t'::regclass, 'base_bare_t'::regclass]);
SELECT CDB_Overviews('polyg_t'::regclass);
SELECT CDB_Overviews('column_types_t'::regclass);

-- Disable overviews - no results should be expected
INSERT INTO cdb_conf VALUES ('disable_overviews', 'true'::json);
SELECT CDB_Overviews('base_t'::regclass);
SELECT CDB_Overviews('"public"."base_t"'::regclass);
SELECT CDB_Overviews(ARRAY['base_t'::regclass, 'base_bare_t'::regclass]);
SELECT CDB_Overviews('polyg_t'::regclass);
SELECT CDB_Overviews('column_types_t'::regclass);

-- Reenable overviews with "false" value in table
UPDATE cdb_conf SET value = 'false'::json WHERE key = 'disable_overviews';
SELECT CDB_Overviews('base_t'::regclass);
SELECT CDB_Overviews('"public"."base_t"'::regclass);
SELECT CDB_Overviews(ARRAY['base_t'::regclass, 'base_bare_t'::regclass]);
SELECT CDB_Overviews('polyg_t'::regclass);
SELECT CDB_Overviews('column_types_t'::regclass);

-- Remove disable flag completely - overviews should be reenabled
DELETE FROM cdb_conf WHERE key = 'disable_overviews';
SELECT CDB_Overviews('base_t'::regclass);
SELECT CDB_Overviews('"public"."base_t"'::regclass);
SELECT CDB_Overviews(ARRAY['base_t'::regclass, 'base_bare_t'::regclass]);
SELECT CDB_Overviews('polyg_t'::regclass);
SELECT CDB_Overviews('column_types_t'::regclass);

SELECT CDB_DropOverviews('column_types_t'::regclass);
SELECT CDB_DropOverviews('base_bare_t'::regclass);
SELECT CDB_DropOverviews('base_t'::regclass);
SELECT count(*) FROM _vovw_2_base_t;

SELECT CDB_CreateOverviewsWithToleranceInPixels('base_t'::regclass, 7.5);
SELECT count(*) FROM _vovw_2_base_t;
SELECT CDB_DropOverviews('base_t'::regclass);

DROP TABLE column_types_t;
DROP TABLE base_bare_t;
DROP TABLE base_t;
DROP TABLE polyg_t;
