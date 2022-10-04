/*
Create empty demographics events table for evaluation.
*/

set role :role;

drop table if exists semantic.demographics_eval;

create table semantic.demographics_eval (
	joid int,
    table_name varchar,
	demographics_type varchar,
    demographics_value varchar
);
