#!/bin/sh
#-----------------------------------------------------
# Descrption : 
#-----------------------------------------------------
#
# Script Name:
# Usage		 :
# Dependecy  :
#
#-----------------------------------------------------
# Date 		  Developer				 Change Descrption
#-----------------------------------------------------
#13-May-21		Venkat 				 Initial Draft
#--
#mysql -h "localhost" -u root "-pHarsha.123$" "venkat_mysql" < "filename.sql" | tee -a ${d_log}

source script.param



db=venkat_mysql
tb1_path='/Users/arjunkummetha/Desktop/Venkat_Project_work/files/innovators.csv'


mysql -h "localhost" -u $userid -p$pwd "venkat_mysql"  | tee -a ${d_log}




create database if not exists ventkat_mysql;


TRUNCATE TABLE $db.innovators1;
TRUNCATE TABLE $db.innovators2;
TRUNCATE TABLE $db.innovators3;
TRUNCATE TABLE $db.innovators4;
TRUNCATE TABLE $db.innovators5;

create table if not exists ventkat_mysql.innovators1
	(
		id int,
		name char,
		contribution char);

create table if not exists ventkat_mysql.innovators2
	(
		id int,
		name char,
		contribution char);
create table if not exists ventkat_mysql.innovators3
	(
		id int,
		name char,
		contribution char);

create table if not exists ventkat_mysql.innovators4
	(
		id int,
		name char,
		contribution char);

create table if not exists ventkat_mysql.innovators5
	(
		id int,
		name char,
		contribution char);


LOAD DATA INFILE '/Users/arjunkummetha/Desktop/Venkat_Project_work/files/innovators1.csv' INTO TABLE ventkat_mysql.innovators1 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA INFILE '/Users/arjunkummetha/Desktop/Venkat_Project_work/files/innovators2.csv' INTO TABLE ventkat_mysql.innovators1 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA INFILE '/Users/arjunkummetha/Desktop/Venkat_Project_work/files/innovators3.csv' INTO TABLE ventkat_mysql.innovators1 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA INFILE '/Users/arjunkummetha/Desktop/Venkat_Project_work/files/innovators4.csv' INTO TABLE ventkat_mysql.innovators1 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA INFILE '/Users/arjunkummetha/Desktop/Venkat_Project_work/files/innovators5.csv' INTO TABLE ventkat_mysql.innovators1 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;


