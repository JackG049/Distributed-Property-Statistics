#!/bin/bash
docker exec database_db_1 /usr/bin/mysqldump -u root --password=password daft_ie > backup.sql

