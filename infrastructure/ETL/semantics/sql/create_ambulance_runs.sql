/*
Create ambulance runs semantic table.
*/

set role :role;

drop table if exists semantic.ambulance_runs;

create table semantic.ambulance_runs as
select
	joid,
	incident_date as event_date,
	primary_impression,
	secondary_impression,
	chief_complaint,
	suicidal_flag,
	suicide_attempt_flag,
	drug_flag,
	other_mental_crisis_flag,
	alcohol_flag,
	drug_poisoning_flag,
	'joco110hsccclientmisc2eaimpression' as table_name
from clean.joco110hsccclientmisc2eaimpression jhe
union
select
	joid,
	incident_date as event_date,
	primary_impression,
	secondary_impression,
	chief_complaint,
	suicidal_flag,
	suicide_attempt_flag,
	drug_flag,
	other_mental_crisis_flag,
	alcohol_flag,
	drug_poisoning_flag,
	'jocomedactincidents' as table_name
from clean.jocomedactincidents j;
