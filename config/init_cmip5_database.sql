# to be run to initialize database if it doesn't exist
# run with mysql -u root -p < init_cmip5_database.sql

CREATE DATABASE cmip5;

use cmip5;

CREATE TABLE paths (pid int NOT NULL AUTO_INCREMENT, path varchar(255), product varchar(255), institute varchar(255), model varchar(255), experiment
varchar(255), tfreq varchar(255), realm varchar(255), cmipTable varchar(255), realization varchar(255), version varchar(255), variable
varchar(255), created DATETIME, modified DATETIME, accessed DATETIME, xmlFile varchar(255), xmlwritedatetime DATETIME, cdscanerror VARCHAR(255),
retired TINYINT, retire_datetime DATETIME, PRIMARY KEY(pid));

-- example user + permissions
-- CREATE USER 'newuser'@'localhost' IDENTIFIED BY 'password';
-- GRANT ALL PRIVILEGES ON cmip5.* TO 'newuser'@'localhost';