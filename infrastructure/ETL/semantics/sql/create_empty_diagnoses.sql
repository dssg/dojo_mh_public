/*
Create empty diagnoses table in semantics schema.
*/

set role :role;

drop table if exists semantic.diagnoses;

create table semantic.diagnoses (
	joid int,
	diag_date date,
    prim_diag varchar,
    sec_diag varchar,
    prim_diag_icd10 varchar,
    sec_diag_icd10 varchar,
    mh_diag_flag bool,
    prim_diag_class varchar,
    sec_diag_class varchar,
    table_name varchar
);