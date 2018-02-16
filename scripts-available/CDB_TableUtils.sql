--
-- Transactionally update the contents of the object identified
-- `dest_table_name`.  The implementation of this update may vary
-- depending on whether or not the table has objects which
-- depend on it.  If possible, the table will be dropped and
-- replaced with the newly imported table by renaming it in
-- constant time.  Otherwise, a truncate insert will be performed
-- when dropping the table is not possible because e.g. the table
-- has dependent objects.
--
CREATE OR REPLACE FUNCTION public.CDB_TableUtils_ReplaceTableContents(
  schema_name text,       -- name of destination schema
  dest_table_name text,   -- name of existing
  source_table_name text, -- fully qualified source table
  swap_table_name text    -- temporary table to use for swapping
)
RETURNS void
AS $$
DECLARE
  has_dependents boolean;
  dest_table regclass;

  column_list text;
BEGIN

  dest_table = FORMAT('%I.%I', schema_name, dest_table_name)::regclass;
  has_dependents := EXISTS(
    SELECT dependent_name
    FROM public.CDB_TableMetadata_DependentViews(ARRAY[dest_table])
  );

  IF has_dependents
  THEN
    -- Because the table has dependents, it must be
    -- updated through a truncate-insert as opposed
    -- to renamed.

    -- Get columns in table
    SELECT
      string_agg(quote_ident(a.attname), ',')
    INTO column_list
    FROM pg_catalog.pg_attribute a
    WHERE a.attnum > 0
      AND NOT a.attisdropped
      AND a.attrelid = dest_table::oid;

    -- Truncate table
    EXECUTE FORMAT('TRUNCATE TABLE %I.%I', schema_name, dest_table_name);

    -- Generate insert select stmt
    EXECUTE FORMAT($q$
      INSERT INTO %I.%I ( %s )
        SELECT %s
        FROM %I
    $q$, schema_name, dest_table_name, column_list, column_list, source_table_name);

    -- 4. Drop source table
    EXECUTE FORMAT('DROP TABLE %I', source_table_name);
  ELSE
    -- The table is safe to replace with the source
    EXECUTE FORMAT($q$
      ALTER TABLE IF EXISTS %I.%I
        RENAME TO %I;

      DROP TABLE IF EXISTS %I.%I;

      ALTER TABLE IF EXISTS %I.%I
        RENAME TO %I;
   $q$, schema_name, dest_table_name, swap_table_name,
        schema_name, swap_table_name,
        schema_name, source_table_name, dest_table_name);
  END IF;

END;
$$ LANGUAGE plpgsql;

