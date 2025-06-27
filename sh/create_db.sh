#!/bin/bash

source /Users/ilya/Desktop/neoFlex/.env
export DB_HOST
export DB_NAME
export DB_USER
export DB_PASSWORD
export PGPASSWORD=$DB_PASSWORD

/Library/PostgreSQL/17/bin/psql -U $DB_USER -h $DB_HOST -d $DB_NAME -f /Users/ilya/Desktop/neoFlex/sql_scripts/script_create_db.sql