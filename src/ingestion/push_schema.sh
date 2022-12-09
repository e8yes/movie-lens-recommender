#!/bin/bash

# Ensure that the PostgreSQL DB engine is started.
sudo systemctl start postgresql

set +e # The database might have already existed, but that's fine.
echo "CREATE DATABASE ingestion WITH TEMPLATE = template0 ENCODING = 'UTF8';" | sudo -u postgres psql postgres
set -e

echo "ALTER DATABASE ingestion OWNER TO postgres;" | sudo -u postgres psql ingestion
echo "ALTER USER postgres WITH PASSWORD '0f21e4cd-44f8-48ab-b112-62030d7f7df1';" | sudo -u postgres psql ingestion
cat 'schema/ingestion.pgsql' | sudo -u postgres psql ingestion

# Ensure that the Cassandra DB engine is started.
sudo systemctl start cassandra
echo "CREATE KEYSPACE IF NOT EXISTS ingestion WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };" | cqlsh
cat 'schema/ingestion.cql' | cqlsh
