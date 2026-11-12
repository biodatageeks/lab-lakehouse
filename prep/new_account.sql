-- ============================================================================================
-- New trial account? Rebuild your lab here. (The old trial expired after 30 days.)
--
-- 1. Sign up for a new Snowflake trial (Enterprise, any AWS region, no card).
-- 2. Projects > Workspaces > + SQL file. Paste prep/bootstrap.sql and Run All.
--    That creates the warehouse, database SHOP, the stages and two procedures.
-- 3. Get the course files into the account, ONE of:
--      a) public repo:  CALL SHOP.OPS.LOAD_COURSE('https://github.com/<org>/lab-lakehouse.git@<branch>');
--      b) upload:       Data > Databases > SHOP > OPS > Stages > COURSE > + Files: upload the repo's
--                       notebooks/ and dbt_shop/ folders (keep the folder names), then
--                       CALL SHOP.OPS.LOAD_COURSE('');
-- 4. Rebuild the state up to the last session you finished (data is deterministic, so you get
--    exactly the tables and grades you had). ~2 min per notebook; 5 = up to dbt (~15 min).
-- 5. Continue with the next session's notebook in Workspaces.
-- ============================================================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE LAB_WH;

-- step 3 (pick one)
CALL SHOP.OPS.LOAD_COURSE('');
-- CALL SHOP.OPS.LOAD_COURSE('https://github.com/biodatageeks/lab-lakehouse.git@snowflake-notebooks');

-- step 4: last finished session (1..7)
CALL SHOP.OPS.REBUILD(5);

-- check: what each notebook reported, newest first
SELECT notebook, ts, payload FROM SHOP.OPS.NB_RESULTS ORDER BY ts DESC LIMIT 20;
