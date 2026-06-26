-- This script runs automatically on first PostgreSQL container initialization.
-- It creates separate databases for each microservice.

CREATE DATABASE audit_db;
CREATE DATABASE portfolio_db;
