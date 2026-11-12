-- Session 6b: read-only access for engines OUTSIDE Snowflake (Google Colab, Kaggle, Codespaces).
-- Run once in a Workspaces SQL file AFTER Session 6 (which publishes GOLD.FCT_ORDERS_ICEBERG).
-- First run the first cell of notebooks/colab/06b_open_engines.ipynb: it creates a key pair and
-- prints the public key to paste below. The private key stays in Colab Secrets.
USE ROLE ACCOUNTADMIN;

-- A role that can only read the course's Iceberg tables (Horizon serves Iceberg tables only).
CREATE ROLE IF NOT EXISTS LAB_READER;
GRANT USAGE ON DATABASE SHOP TO ROLE LAB_READER;
GRANT USAGE ON SCHEMA SHOP.GOLD TO ROLE LAB_READER;
GRANT USAGE ON SCHEMA SHOP.SILVER TO ROLE LAB_READER;
GRANT SELECT ON ALL ICEBERG TABLES IN SCHEMA SHOP.GOLD TO ROLE LAB_READER;
GRANT SELECT ON ALL ICEBERG TABLES IN SCHEMA SHOP.SILVER TO ROLE LAB_READER;
GRANT SELECT ON FUTURE ICEBERG TABLES IN SCHEMA SHOP.GOLD TO ROLE LAB_READER;
GRANT SELECT ON FUTURE ICEBERG TABLES IN SCHEMA SHOP.SILVER TO ROLE LAB_READER;
GRANT USAGE ON WAREHOUSE LAB_WH TO ROLE LAB_READER;   -- for the Snowflake SQL reference query

-- A service user for the notebook: key-pair auth only (no password, no MFA prompt).
CREATE USER IF NOT EXISTS LAB_COLAB TYPE = SERVICE DEFAULT_ROLE = LAB_READER DEFAULT_WAREHOUSE = LAB_WH
  COMMENT = 'External notebook engines (Colab) via Horizon Iceberg REST';
GRANT ROLE LAB_READER TO USER LAB_COLAB;
-- Paste the public key the Colab notebook printed (one line, without the BEGIN/END lines):
ALTER USER LAB_COLAB SET RSA_PUBLIC_KEY = '<paste the public key here>';

-- After the course, revoke access with ALTER USER LAB_COLAB UNSET RSA_PUBLIC_KEY or DROP USER LAB_COLAB
