-- This tests file includes tests for coordinator tables.

-- \set VERBOSITY terse

SET citus.next_shard_id TO 1504000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;
SET citus.log_local_commands TO ON;

CREATE SCHEMA single_placement_tables_test_schema;
SET search_path TO single_placement_tables_test_schema;

-- let coordinator have coordinator tables
set client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

-------------------------------------------------
------- SELECT / INSERT / UPDATE / DELETE -------
-------------------------------------------------

CREATE TABLE coordinator_table(a int, b int);
SELECT create_coordinator_table('coordinator_table');

CREATE TABLE coordinator_table_2(a int, b int);
SELECT create_coordinator_table('coordinator_table_2');

CREATE TABLE reference_table(a int, b int);
SELECT create_reference_table('reference_table');

CREATE TABLE distributed_table(a int, b int);
SELECT create_distributed_table('distributed_table', 'a');

CREATE TABLE local_table(a int, b int);

-----------------------------------------------------------
----- ok tests with local tables and reference tables -----
-----------------------------------------------------------

DELETE FROM coordinator_table
USING local_table
WHERE coordinator_table.b = local_table.b;

UPDATE coordinator_table
SET b = 5
FROM local_table
WHERE coordinator_table.a = 3 AND coordinator_table.b = local_table.b;

SELECT coordinator_table.b, local_table.a
FROM coordinator_table, local_table
WHERE coordinator_table.a = local_table.b;

SELECT count(*) FROM coordinator_table, reference_table WHERE coordinator_table.a = reference_table.a;

-- XXX: this will also work after IsLocalReferenceTableJoinPlan is removed
INSERT INTO reference_table
SELECT coordinator_table.a, coordinator_table.b
FROM coordinator_table, local_table
WHERE coordinator_table.a > local_table.b;

-- sure it is okay
INSERT INTO coordinator_table
SELECT * FROM coordinator_table_2;

--------------------------------------------
----- fail tests with reference tables -----
--------------------------------------------

INSERT INTO coordinator_table
SELECT * FROM reference_table;

----------------------------------------------
----- some tests with distributed tables -----
----------------------------------------------

-- insert into distributed table is okay
INSERT INTO distributed_table
SELECT * from coordinator_table;

-- join between coordinator table and distributed table would fail
SELECT coordinator_table.a FROM coordinator_table, distributed_table;

-- modification of either of them would also fail as in below two
INSERT INTO coordinator_table
SELECT * from distributed_table ;

UPDATE distributed_table
SET b = 6
FROM coordinator_table
WHERE coordinator_table.a = distributed_table.a;

-- modification of coordinator table would fail
UPDATE coordinator_table
SET b = 6
FROM distributed_table
WHERE coordinator_table.a = distributed_table.a;

-- even if we use subquery on distributed table, below two would fail
DELETE FROM coordinator_table
WHERE coordinator_table.a IN (SELECT a FROM distributed_table);

WITH distributed_table_cte AS (SELECT * FROM distributed_table)
UPDATE coordinator_table
SET b = 6
FROM distributed_table_cte
WHERE coordinator_table.a = distributed_table_cte.a;

--------------------------------
------- UTILITY COMMANDS -------
--------------------------------
-- TODO: below copy & alter table fkey tests are broken as they are not implemented yet

----------------------------------------------------
----- some basic commands on coordinator table -----
----------------------------------------------------

\COPY coordinator_table FROM stdin delimiter ',';
1,2
3,4
6,7
8,9
\.

SELECT COUNT(*) FROM coordinator_table;

TRUNCATE coordinator_table;

--------------------------------------------------------
-- constraint relationships with other kind of tables --
-- test DROP / TRUNCATE execution as well --------------
--------------------------------------------------------

BEGIN;
  -- add another columnd with primary constraint on it
  ALTER TABLE coordinator_table ADD COLUMN c int;

  -- note that we should name this constraint
  ALTER TABLE coordinator_table ADD CONSTRAINT pkey1 PRIMARY KEY(c);

  INSERT INTO coordinator_table VALUES (1, 2, 3);

  -- show that add primary key constaint command was successfull
  -- below should fail
  INSERT INTO coordinator_table VALUES (1, 2, 3);
ROLLBACK;

-- add foreign key constraint with another coordinator table
ALTER TABLE coordinator_table_2 ADD CONSTRAINT pkey2 PRIMARY KEY(a);
ALTER TABLE coordinator_table ADD CONSTRAINT fkey_c2c FOREIGN KEY(a) REFERENCES coordinator_table_2(a);

INSERT INTO coordinator_table_2 VALUES (1, 2);

-- show that add foreign key constaint command was successfull
-- below should be executed successfully
INSERT INTO coordinator_table VALUES (1, 2);

-- but below should fail
INSERT INTO coordinator_table VALUES (2, 2);

-- should fail
DROP TABLE coordinator_table_2;

-- drop / truncate commands will also be handled successfully by local execution
-- below transaction blocks should be executed successfully
BEGIN;
  TRUNCATE coordinator_table_2 CASCADE;
  
  -- show that truncate is handled successfully, should print 0
  SELECT COUNT(*) coordinator_table;

  DROP TABLE coordinator_table_2 CASCADE;

  -- show that drop command is handled successfully
  SELECT 1 FROM pg_tables WHERE tablename='coordinator_table_2';
ROLLBACK;

BEGIN;
  TRUNCATE coordinator_table, coordinator_table_2;
  
  -- show that truncate is handled successfully, should print 0
  SELECT COUNT(*) coordinator_table;

  DROP TABLE coordinator_table, coordinator_table_2;
  
  -- show that drop command is handled successfully, should print 0
  SELECT count(*) FROM pg_tables WHERE tablename like 'coordinator_table_%';
ROLLBACK;

ALTER TABLE reference_table ADD CONSTRAINT pkey3 PRIMARY KEY(a);
ALTER TABLE coordinator_table ADD CONSTRAINT pkey4 PRIMARY KEY(a);

-- define self reference -which also tests foreign keys between coordinator tables-
ALTER TABLE coordinator_table ADD CONSTRAINT fkey_c2c_self FOREIGN KEY(a) REFERENCES coordinator_table(b);

-- for now, we cannot set foreign keys between coordinator tables and reference tables
-- below two should fail
ALTER TABLE coordinator_table ADD CONSTRAINT fkey_c2ref FOREIGN KEY(a) REFERENCES reference_table(a);
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref2c FOREIGN KEY(a) REFERENCES coordinator_table(a);

-- TODO: some sanity tests with distributed tables (or in above ?)
