
set role :role;

create schema if not exists results;

drop table if exists results.model_sets;
drop table if exists results.models;
drop table if exists results.train_predictions;
drop table if exists results.test_predictions;
drop table if exists results.train_evaluations;
drop table if exists results.test_evaluations;
drop table if exists results.feature_importance;

create table results.model_sets (
    model_set_id serial,
	experiment_id int,
    type varchar,
    params jsonb,
	temporal_params jsonb
);

create table results.models (
	model_id serial,
	model_set_id int,
	train_end_date date
);

create table results.train_predictions (
	model_id int,
	joid int,
	county varchar,
	as_of_date date,
	score float,
	label_name varchar,
	label boolean,
	k int,
	county_k int
);

create table results.test_predictions (
	model_id int,
	joid int,
	county varchar,
	as_of_date date,
	score float,
	label_name varchar,
	label boolean,
	k int,
	county_k int
);


create table results.test_evaluations (
	model_id int,
	county varchar,
	as_of_date date,
	metric varchar,
	k int,
	county_k int,
	value float
);

create table results.train_evaluations (
	model_id int,
	county varchar,
	as_of_date date,
	metric varchar,
	k int,
	county_k int,
	value float
);

create table results.feature_importance (
	model_id int,
	train_end_date date,
	feature_name varchar,
	feature_importance float
);

-- Create an index for querying the model_id (individually)
create index concurrently model_id_idx on results.test_predictions (model_id);
create index concurrently model_id_idx on results.feature_importance (model_id);
