
/* This file is meant to clean all the non-clean 110 tables, the aims table, pdarrests tables, and dhencounter.
Currently only cleans jocojcdheencounter, jocoaimsbookings, joco110hsccclientmisc2eachargesn

 currently waiting on information in order to semi clean the rest (i.e. joco110hsccclientmisc2eacharges, joco110hsccclientmisc2, joco110hsccclientmisc2, pdarrests

*/

-- jocojcdheencounter
set role :role;

drop table if exists semi_clean.jocojcdheencounter
create table semi_clean.jocojcdheencounter as (
with raw_table as (
	select
	urno,
	patient_no,
	hash_name_last,
	hash_name_first,
	hash_name_mid,
	dob as dob_raw,
	hash_address,
	hash_address_2,
	city,
	state,
	zip_code,
	hash_home_phone,
	race as race_raw,
	sex as sex_raw,
	hash_ssn1_9,
	"program",
	hash_provider,
	encounter_date,
	hash_mothers_last,
	hash_mothers_first,
	hash_fathers_last,hash_fathers_first,
	hash_respon_last,
	hash_respon_first,
	hash_respon_mid,
	cosite,
	cnt,
	rn,
	tract2010id,
	blockgroup2010id,
	block2010id
	from raw.jocojcdheencounter
)
select
c.joid as joid,
fix_date(t.encounter_Date::date::varchar) as event_date,
fix_date(t.dob_raw::date::varchar) as dob,
fix_race(t.race_raw) as race,
fix_sex(t.sex_raw) as sex,
t.*
from raw_table t
join clean.jocojococlient c
on c.sourceid = t.patient_no::varchar
where c.source = 'JOCOJCDHEENCOUNTER.PATIENT_NO'
)

--jocoaimsbookings
drop table if exists semi_clean.jocoaimsbookings;

create table semi_clean.jocoaimsbookings as (
with raw_table as (
select
urno,
id,
type,
hash_lastname,
hash_firstname,
hash_middlename,
race as race_raw,
sex as sex_raw,
dob as dob_raw,
hash_address,
city,
state,
zip,
hash_cfn,
hash_bookingnumber,
bookingofficer,
bookingdate,
bookingtime,
agency,
hash_arrestingofficer,
arrestdate,
arresttime,
releasedate,
releasetime,
releasetype,
releaselocation,
charge,
chargedesc,
chargetype,
chargelevel,
dispo,
bond,
warrant,
hash_comments,
aimsinsertdatetime
from raw.jocoaimsbookings
)
select
c.joid as joid,
fix_date(t.aimsinsertdatetime::date::varchar) as event_date,
fix_date(t.dob_raw) as dob,
fix_race(t.race_raw) as race,
fix_sex(t.sex_raw) as sex,
t.*
from raw_table t
join clean.jocojococlient c
on c.hash_sourceid = t.hash_cfn
where c.source = 'JOCOAIMSBOOKINGS.CFN'
);

-- joco110hsccclientmisc2eacharges
drop table if exists semi_clean.joco110hsccclientmisc2eacharges

create table semi_clean.joco110hsccclientmisc2eacharges as (
select
c.joid,
fix_date(substring(t.intro, 12,12)) as event_date,
t.*
from raw.joco110hsccclientmisc2eacharges t
left join clean.jocojococlient c
on t.clientid::varchar = c.sourceid
and c.source = 'JOCO110HSCCCLIENT2.CLIENTID'
);

--joco110hsccclientservice2
drop table if exists semi_clean.joco110hsccclientservice2

create table semi_clean.joco110hsccclientservice2 as (
select
c.joid,
fix_date(servicebegindate::date::varchar) as event_date,
t.*
from raw.joco110hsccclientservice2 t
left join clean.jocojococlient c
on t.clientid::varchar = c.sourceid
and c.source = 'JOCO110HSCCCLIENT2.CLIENTID'
);