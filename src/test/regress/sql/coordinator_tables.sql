-- This tests file includes tests for coordinator tables.

SET citus.next_shard_id TO 1504000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;
SET citus.log_local_commands TO ON;

CREATE SCHEMA coordinator_tables_test_schema;
SET search_path TO coordinator_tables_test_schema;

------------------------------------------
------- coordinator table creation -------
------------------------------------------

-- ensure that coordinator is added to pg_dist_node
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

CREATE TABLE coordinator_table_1 (a int);

-- this should work as coordinator is added to pg_dist_node
SELECT create_coordinator_table('coordinator_table_1');

-- try to remove coordinator and observe failure as there exist a coordinator table
SELECT 1 FROM master_remove_node('localhost', :master_port);

DROP TABLE coordinator_table_1;

-- this should work
SELECT 1 FROM master_remove_node('localhost', :master_port);

CREATE TABLE coordinator_table_1 (a int);

-- this should fail as coordinator is removed from pg_dist_node
SELECT create_coordinator_table('coordinator_table_1');

-- let coordinator have coordinator tables again for next tests
set client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

-- creating coordinator table having no data initially would work
SELECT create_coordinator_table('coordinator_table_1');

-- creating coordinator table having data in it would also work
CREATE TABLE coordinator_table_2(a int);
INSERT INTO coordinator_table_2 VALUES(1);

SELECT create_coordinator_table('coordinator_table_2');

-- cannot create coordinator table from an existing citus table
CREATE TABLE distributed_table (a int);
SELECT create_distributed_table('distributed_table', 'a');

-- this will error out
SELECT create_coordinator_table('distributed_table');

-- partitiond table tests --

CREATE TABLE partitioned_table(a int, b int) PARTITION BY RANGE (a);
CREATE TABLE partitioned_table_1 PARTITION OF partitioned_table FOR VALUES FROM (0) TO (10);
CREATE TABLE partitioned_table_2 PARTITION OF partitioned_table FOR VALUES FROM (10) TO (20);

-- cannot create partitioned coordinator tables
SELECT create_coordinator_table('partitioned_table');

-- cannot create coordinator tables as a partition of a local table
BEGIN;
  CREATE TABLE coordinator_table PARTITION OF partitioned_table FOR VALUES FROM (20) TO (30);

  -- this should fail
  SELECT create_coordinator_table('coordinator_table');
ROLLBACK;

-- cannot create coordinator tables as a partition of a local table
-- via ALTER TABLE commands as well
BEGIN;
  CREATE TABLE coordinator_table (a int, b int);

  SELECT create_coordinator_table('coordinator_table');

  -- this should fail
  ALTER TABLE partitioned_table ATTACH PARTITION coordinator_table FOR VALUES FROM (20) TO (30);
ROLLBACK;

-- cannot attach coordinator tables to a partitioned distributed table
BEGIN;
  SELECT create_distributed_table('partitioned_table', 'a');

  CREATE TABLE coordinator_table (a int, b int);
  SELECT create_coordinator_table('coordinator_table');

  -- this should fail
  ALTER TABLE partitioned_table ATTACH PARTITION coordinator_table FOR VALUES FROM (20) TO (30);
ROLLBACK;

-- cleanup at exit
DROP SCHEMA coordinator_tables_test_schema CASCADE;
