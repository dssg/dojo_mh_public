/*
Create empty cohort table.
*/

set role {psql_role};

drop table if exists modeling.cohort cascade;

create table modeling.cohort (
    joid int,
    as_of_date date,
    county varchar
);

create index idx_joid_cohort1
on modeling.cohort(joid);

create index idx_as_of_date_cohort1
on modeling.cohort(as_of_date);
