/*
Create semantics table for Johnson county call details.
*/

set role :role;

drop table if exists semantic.joco_calls cascade;

create table semantic.joco_calls as
select
	joid,
	event_date,
	call_type,
	referral_source,
	presenting_issue,
	risk_to_self_value,
	suicide_homicide_risk,
	disposition,
	'jocojcmhccalldetails2021' as table_name
from semi_clean.jocojcmhccalldetails2021 j
union
select
	joid,
	event_date,
	call_type,
	referral_source,
	presenting_issue,
	null as risk_to_self_value,
	suicide_homicide_risk,
	disposition,
	'jocojcmhccalldetails20142020' as table_name
from semi_clean.jocojcmhccalldetails20142020 j2;
