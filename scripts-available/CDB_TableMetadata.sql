
CREATE TABLE IF NOT EXISTS
  public.CDB_TableMetadata (
    tabname regclass not null primary key,
    updated_at timestamp with time zone not null default now()
  );

CREATE OR REPLACE VIEW public.CDB_TableMetadata_Text AS
       SELECT FORMAT('%I.%I', n.nspname::text, c.relname::text) tabname, updated_at
       FROM public.CDB_TableMetadata m JOIN pg_catalog.pg_class c ON m.tabname::oid = c.oid
       LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid;

-- No one can see this
-- Updates are only possible trough the security definer trigger
-- GRANT SELECT ON public.CDB_TableMetadata TO public;


--
-- Get all views which depend, directly or indirectly, on the
-- specified table_names. This is useful for determining e.g.
-- the views in a user database which depend on user tables
-- in order to maintain updated timestamps for such views in
-- CDB_TableMetadata:
--
--  SELECT DISTINCT dependent_oid::regclass, dependent_name
--  FROM CDB_TableMetadata_DependentViews('{a,b,c}'::regclass[])
--  WHERE base_dependency = 'a':regclass;
--

CREATE OR REPLACE FUNCTION public.CDB_TableMetadata_DependentViews(table_names regclass[])
RETURNS TABLE (
  dependent_oid oid,        -- oid of dependent view
  dependency_oid oid,       -- oid of direct dependent of dependent view
  base_dependency_oid oid,  -- oid of original table dependency of dependent view
  dependent_name text,      -- name of dependent view
  dependency_name text,     -- name of direct dependent of dependent view
  base_dependency_name text -- name of original table dependency of dependent view
)
AS
$$
BEGIN

  RETURN QUERY
  WITH RECURSIVE dependent_views AS (
    -- Direct dependent views of tables
    SELECT
      v.oid as dependent_oid,
      t.oid as dependency_oid,
      t.oid as base_dependency_oid,
      v.oid::regclass::text as dependent_name,
      t.oid::regclass::text as dependency_name,
      t.oid::regclass::text as base_dependency_name
    FROM pg_depend d
    JOIN pg_class t
      ON t.oid = d.refobjid
      AND t.oid::regclass = ANY(table_names)
    JOIN pg_rewrite rw
      ON rw.oid = d.objid
    JOIN pg_class v
      ON rw.ev_class = v.oid
    -- Ignore self dependencies
    WHERE v.oid <> t.oid

    UNION ALL

    -- Dependent views of dependent views
    SELECT
      v.oid as dependent_oid,
      dv.dependent_oid as dependency_oid,
      dv.base_dependency_oid as base_dependency_oid,
      v.oid::regclass::text as dependent_name,
      dv.dependent_oid::regclass::text as dependency_name,
      dv.base_dependency_oid::regclass::text as base_dependency_name
    FROM pg_depend d
    JOIN dependent_views dv
      ON dv.dependent_oid = d.refobjid
    JOIN pg_rewrite rw
      ON rw.oid = d.objid
    JOIN pg_class v
      ON rw.ev_class = v.oid
    -- Ignore self dependencies
    WHERE v.oid <> dv.dependent_oid
  )
  SELECT DISTINCT
    dv.dependent_oid,
    dv.dependency_oid,
    dv.base_dependency_oid,
    dv.dependent_name,
    dv.dependency_name,
    dv.base_dependency_name
  FROM dependent_views dv;

END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

--
-- Trigger logging updated_at in the CDB_TableMetadata
-- and notifying cdb_tabledata_update with table name as payload.
--
-- This maintains the update timestamps for the table to which
-- it is attached as well as any views which reference the table.
--
-- Attach to tables like this:
--
--   CREATE trigger track_updates
--    AFTER INSERT OR UPDATE OR TRUNCATE OR DELETE ON <tablename>
--    FOR EACH STATEMENT
--    EXECUTE PROCEDURE cdb_tablemetadata_trigger(); 
--
-- NOTE: _never_ attach to CDB_TableMetadata ...
--
CREATE OR REPLACE FUNCTION CDB_TableMetadata_Trigger()
RETURNS trigger AS
$$
BEGIN
  -- Guard against infinite loop
  IF TG_RELID = 'public.CDB_TableMetadata'::regclass::oid THEN
    RETURN NULL;
  END IF;

  -- Cleanup stale entries
  DELETE FROM cdb_tablemetadata m
  USING (
     SELECT tabname
     FROM cdb_tablemetadata
     WHERE NOT EXISTS (
       SELECT oid FROM pg_class WHERE oid = tabname
     )
     ORDER BY tabname ASC
     FOR UPDATE
  ) d
  WHERE d.tabname = m.tabname;

  -- Update metadata for table
  PERFORM public.CDB_TableMetadataTouch(TG_RELID::regclass);

  RETURN NULL;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

--
-- Trigger invalidating varnish whenever CDB_TableMetadata
-- record change.
--
CREATE OR REPLACE FUNCTION _CDB_TableMetadata_Updated()
RETURNS trigger AS
$$
DECLARE
  tabname regclass;
  rec RECORD;
  found BOOL;
  schema_name TEXT;
  table_name TEXT;
BEGIN

  IF TG_OP = 'UPDATE' or TG_OP = 'INSERT' THEN
    tabname = NEW.tabname;
  ELSE
    tabname = OLD.tabname;
  END IF;

  -- Notify table data update
  -- This needs a little bit more of research regarding security issues
  -- see https://github.com/CartoDB/cartodb/pull/241
  -- PERFORM pg_notify('cdb_tabledata_update', tabname);

  --RAISE NOTICE 'Table % was updated', tabname;

  -- This will be needed until we'll have someone listening
  -- on the event we just broadcasted:
  --
  --  LISTEN cdb_tabledata_update;
  --

  -- Call the first varnish invalidation function owned
  -- by a superuser found in cartodb or public schema
  -- (in that order)
  found := false;
  FOR rec IN SELECT u.usesuper, u.usename, n.nspname, p.proname
             FROM pg_proc p, pg_namespace n, pg_user u
             WHERE p.proname = 'cdb_invalidate_varnish'
               AND p.pronamespace = n.oid
               AND n.nspname IN ('public', 'cartodb')
               AND u.usesysid = p.proowner
               AND u.usesuper
             ORDER BY n.nspname
  LOOP
    SELECT n.nspname, c.relname FROM pg_class c, pg_namespace n WHERE c.oid=tabname AND c.relnamespace = n.oid INTO schema_name, table_name;
    EXECUTE 'SELECT ' || quote_ident(rec.nspname) || '.'
            || quote_ident(rec.proname)
            || '(' || quote_literal(quote_ident(schema_name) || '.' || quote_ident(table_name)) || ')';
    found := true;
    EXIT;
  END LOOP;
  IF NOT found THEN RAISE WARNING 'Missing cdb_invalidate_varnish()'; END IF;

  RETURN NULL;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

DROP TRIGGER IF EXISTS table_modified ON public.CDB_TableMetadata;
-- NOTE: on DELETE we would be unable to convert the table
--       oid (regclass) to its name
CREATE TRIGGER table_modified AFTER INSERT OR UPDATE
ON public.CDB_TableMetadata FOR EACH ROW EXECUTE PROCEDURE
 _CDB_TableMetadata_Updated();


-- similar to TOUCH(1) in unix filesystems but for table in cdb_tablemetadata
CREATE OR REPLACE FUNCTION public.CDB_TableMetadataTouch(tablename regclass)
    RETURNS void AS
    $$
    BEGIN
        WITH nv as (
          SELECT tablename as tabname, NOW() as t
        ), all_dependents as (
          SELECT nv.tabname, nv.t
          FROM nv
          UNION
          SELECT
            dv.dependent_oid as tabname,
            nv.t
          FROM nv, public.CDB_TableMetadata_DependentViews(ARRAY[nv.tabname::regclass]) dv
        ), updated as (
          UPDATE public.CDB_TableMetadata x SET updated_at = ad.t
          FROM (
            SELECT
              md.tabname,
              ad.t
            FROM public.CDB_TableMetadata md
            JOIN all_dependents ad
              ON ad.tabname = md.tabname
            ORDER BY md.tabname ASC
            FOR UPDATE OF md
          ) ad
          WHERE x.tabname = ad.tabname
          RETURNING x.tabname
        )
        INSERT INTO public.CDB_TableMetadata SELECT ad.*
        FROM all_dependents ad
        LEFT JOIN updated u
          ON u.tabname = ad.tabname
        LEFT JOIN public.CDB_TableMetadata i
          ON i.tabname = ad.tabname
        WHERE u.tabname IS NULL
          AND i.tabname IS NULL;
    END;
    $$
LANGUAGE 'plpgsql' VOLATILE STRICT;
