-- This file was automatically created by the migrator to setup helper functions
-- and other internal bookkeeping. This file is safe to edit, any future
-- changes will be added to existing projects as new migrations.
DROP FUNCTION IF EXISTS pgm_manage_updated_at(_tbl regclass);

DROP FUNCTION IF EXISTS pgm_set_updated_at();
