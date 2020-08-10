\set VERBOSITY terse

SET citus.next_shard_id TO 1508000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;
SET citus.log_local_commands TO ON;

CREATE SCHEMA citus_local_tables_mx;
SET search_path TO citus_local_tables_mx;

-- ensure that coordinator is added to pg_dist_node
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

CREATE TABLE citus_local_table (value int);
SELECT create_citus_local_table('citus_local_table');

-- first stop metadata sync to worker_1
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);

CREATE FUNCTION dummy_function() RETURNS trigger AS $dummy_function$
BEGIN
    RAISE EXCEPTION 'a trigger that throws this exception';
END;
$dummy_function$ LANGUAGE plpgsql;

CREATE TRIGGER dummy_function_trigger
BEFORE UPDATE OF value ON citus_local_table
FOR EACH ROW EXECUTE FUNCTION dummy_function();

-- Show that we can sync metadata successfully. That means, we create
-- the function that trigger needs in mx workers too.
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

CREATE EXTENSION seg;
ALTER TRIGGER dummy_function_trigger ON citus_local_table DEPENDS ON EXTENSION seg;
ALTER TRIGGER dummy_function_trigger ON citus_local_table RENAME TO renamed_trigger;
ALTER TABLE citus_local_table DISABLE TRIGGER ALL;
-- show that update trigger mx relation are depending on seg, renamed and disabled.
-- both workers should should print 1.
SELECT run_command_on_workers(
$$
SELECT COUNT(*) FROM pg_depend, pg_trigger, pg_extension
WHERE pg_trigger.tgrelid='citus_local_tables_mx.citus_local_table'::regclass AND
      pg_trigger.tgname='renamed_trigger' AND
      pg_trigger.tgenabled='D' AND
      pg_depend.classid='pg_trigger'::regclass AND
      pg_depend.deptype='x' AND
      pg_trigger.oid=pg_depend.objid AND
      pg_extension.extname='seg'
$$);

CREATE FUNCTION another_dummy_function() RETURNS trigger AS $another_dummy_function$
BEGIN
    RAISE EXCEPTION 'another trigger that throws another exception';
END;
$another_dummy_function$ LANGUAGE plpgsql;
-- Show that we can create the trigger successfully. That means, we create
-- the function that trigger needs in mx worker too when processing CREATE
-- TRIGGER commands.
CREATE TRIGGER another_dummy_function_trigger
AFTER TRUNCATE ON citus_local_table
FOR EACH STATEMENT EXECUTE FUNCTION another_dummy_function();

-- cleanup at exit
DROP SCHEMA citus_local_tables_mx CASCADE;
