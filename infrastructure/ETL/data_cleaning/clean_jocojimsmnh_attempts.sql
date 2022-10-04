/*
Script to clean JoCo JIMS mnh attempts

Comments:
    - A few invalid dates are present and have their values set to null.
*/

set role :role;

drop table if exists clean.jocojimsmnh_attempts cascade;

create table clean.jocojimsmnh_attempts as (
select distinct
	c.joid,
	encode(ja.hash_mnh_no_0, 'hex') as hash_mnh,
	case
		when char_length(regexp_replace(attempt_no__22, '[^0-9]', '', 'g')) <> 1 then null
		else cast(regexp_replace(attempt_no__22, '[^0-9]', '', 'g') as smallint)
	end as attempt_no,
	--- next two are stored as chars
	fix_date(fix_char(case when attempt_date_24 in ('12/1417', '4/34/17') then null else attempt_date_24 end)) as attempt_date,
	fix_char(attempt_time_25) as attempt_time,
	--- the following is stored as a date if the date and time combination is valid
	case
		when char_length(regexp_replace(attempt_date_24, '[^0-9]', '', 'g')) not in (6, 8) then null
		when is_date(concat(attempt_date_24, ' ', attempt_time_25)) then to_timestamp(concat(attempt_date_24, ' ', attempt_time_25), 'MM/DD/YYYY HH24:MI')::timestamp
		else null
	end as attempt_datetime,
	fix_char(outcome_27) as outcome,
	fix_char(contact_type_26) as contact_type,
	encode(hash_staff_23, 'hex') as hash_staff
from
	raw.jocojimsmnh_attempts ja
left join raw.jocojimsmnhdata d
	on d.hash_mnh_no_0 = ja.hash_mnh_no_0
left join clean.jocojococlient c
	on c.sourceid = d.mni_13
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0'
);
