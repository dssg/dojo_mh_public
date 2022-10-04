--------------------------------------------------------------------------------------------
-- Cleans Police Department Arrests in Johnson county, stores it in schema 'clean'
-- Tables: jocopdarrests
-- Adapted from the previous DSSG project (suggests >100,000 people are not JoCo residents)
--------------------------------------------------------------------------------------------
set role :role;
drop table if exists clean.jocopdarrests;

create table clean.jocopdarrests as (
select distinct
    client.joid,
    armainid,
    fix_varchar(name_id::varchar) as name_id,
    time_to_date(date_arr) as date_arrest,
    fix_char(charge) as charge,
    fix_char(arr_type) as arr_type,
    fix_char(agency) as agency,
    time_to_date(arr.dob) as date_of_birth,
    joco_resident(
          arresteetract2010id,
          encode(hash_streetnbr, 'hex') || encode(hash_street, 'hex')
    ) as joco_resident,
    fix_city(remove_ks(fix_char(city))) as city,
    fix_state(fix_char(state)) as state,
    nullif(regexp_replace(zip, '\D*', ''), '') as zip,
    fix_city(remove_ks(fix_char(nm_city))) as arrest_city,
    fix_state(fix_char(nm_state)) as arrest_state,
    nullif(regexp_replace(nm_zip, '\D*', ''), '') as arrest_zip
from raw.jocopdarrests arr
left join clean.jocojococlient client
on client.source = 'JOCOPDARRESTS.NAME_ID'
and client.sourceid = arr.name_id::varchar
);
