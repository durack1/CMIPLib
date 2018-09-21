# to be run to initialize database if it doesn't exist
# run with mysql -u root -p < init_cmip_database.sql

CREATE DATABASE cmip_data;

use cmip_data;

CREATE TABLE paths (pid int NOT NULL AUTO_INCREMENT, path varchar(255), mip_era varchar(255), activity varchar(255), institute varchar(255), model varchar(255), 
	experiment varchar(255), member varchar(255), cmipTable varchar(255), realm varchar(255), frequency varchar(255), variable varchar(255), grid varchar(255), gridLabel varchar(255), 
	version varchar(255), created DATETIME, modified DATETIME, accessed DATETIME, xmlFile varchar(255), xmlwritedatetime DATETIME, cdscanerror VARCHAR(255),
	retired TINYINT, retire_datetime DATETIME, ignored TINYINT, ignored_datetime DATETIME, PRIMARY KEY(pid));

CREATE TABLE invalid_paths (pid int NOT NULL AUTO_INCREMENT, path varchar(255), datetime DATETIME, PRIMARY KEY(pid));

CREATE TABLE stats (pid int NOT NULL AUTO_INCREMENT, indicator varchar(255), value int, datetime DATETIME, PRIMARY KEY(pid));

-- example user + permissions
-- CREATE USER 'newuser'@'localhost' IDENTIFIED BY 'password';
-- GRANT ALL PRIVILEGES ON cmip_data.* TO 'newuser'@'localhost';

-- probably not needed: set global max_allowed_packet=67108864;
-- /etc/init.d/mysqld restart