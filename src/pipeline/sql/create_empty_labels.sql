/*
Query to create the labels table.
*/

set role {psql_role};

drop table if exists modeling.{label_tablename} cascade;

create table modeling.{label_tablename} (
    joid int,
    as_of_date date,
    county varchar,
    label_name varchar,
    label boolean
);

create index idx_joid_{label_tablename}
on modeling.{label_tablename}(joid);

create index idx_county_{label_tablename}
on modeling.{label_tablename}(county);

create index idx_joid_as_of_date_county_{label_tablename}
on modeling.{label_tablename}(joid, as_of_date, county);

create index idx_as_of_date_{label_tablename}
on modeling.{label_tablename}(as_of_date);
