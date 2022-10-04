/*
Clean JoCo JIMS MNH data.

Comments:
	- A few invalid dates are present and have their values set to null.
*/

set role :role;

drop table if exists clean.jocojimsmnhdata cascade;

create table clean.jocojimsmnhdata as (
select distinct
	joid,
	encode(hash_mnh_no_0, 'hex') as hash_mnh,
	--- sanitize the following dates
	fix_and_check_date(connect_dt_28) as connect_dt,
	fix_and_check_date(dob_11) as dob,
	fix_and_check_date(book_date_12) as book_date,
	fix_and_check_date(connect_tm_29) as connect_tm,
	fix_and_check_date(rel_date_14) as rel_date,
	fix_time(rel_time_15) as rel_time,
	fix_sex(gender_10) as gender,
	fix_char(jcmhc_id_16) as jcmhc_id,
	fix_char(mhc_status_17) as mhc_status,
	fix_char(mcrt_assert_18) as mcrt_assert,
	fix_char(mcrt_why_19) as mcrt_why,
	fix_char(created_20 ) as created,
	fix_char(updated_21) as updated,
	-- This column does not appear very useful or clean
	-- fix_char(notified_team_31) as notified_team_3,
	fix_char(final_dispo_33) as final_dispo,
	fix_char(came_to_aoi_34) as came_to_aoi,
	fix_city(city_6) as city,
	fix_zip(zip_8) as zip,
	fix_char(mni_13) as mni,
	-- Add a function to handle dates in the above way
	fix_real(elapsed_hrs_30) as elapsed_hrs
from
	raw.jocojimsmnhdata d
left join clean.jocojococlient c
	on c.sourceid = d.mni_13
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0'
);
