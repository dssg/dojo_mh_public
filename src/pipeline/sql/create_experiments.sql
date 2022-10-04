set role :role;

create table if not exists results.experiments (
    experiment_id serial,
	starts timestamp,
	ends timestamp,
    config jsonb,
    trained_on varchar,
    label_group varchar
);
