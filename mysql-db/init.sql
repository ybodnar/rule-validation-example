create database if not exists db;
use db;

drop table if exists stores;
create table stores (
  id int auto_increment primary key,
  address varchar(255),
  province_code char(2),
  postal_code varchar(6)
);


drop table if exists rules_results;
create table rules_results (
  id int,
  entity varchar(255),
  attribute varchar(255),
  rule_name varchar(255),
  rule_result tinyint
);

drop table if exists rules;
create table rules (
  id int auto_increment primary key,
  rule_name varchar(255),
  rule_dsl varchar(255)
);


insert into db.stores (address, province_code, postal_code)
values ("21 Webb Drive", "ON", "B3A4W2"),
("4400 Eglinton West", "ON", "B3A4W3");

insert into db.rules (rule_name, rule_dsl)
values ("validators_province_code", "val in ('ON', 'AB','BC')"),
      ("validators_postal_code", "len(val) == 6" );
