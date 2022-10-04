/*
Create clean table for jocojcmhcservices (Johnson County MHC services).

Changes:
- Add joid from jocojococlient table
- Convert patid to varchar for ease of table joins
- Turn service description and sourcesystem variable to upper case (standardizing varchars)
*/

set role :role;

drop table if exists clean.jocojcmhcservices cascade;

create table clean.jocojcmhcservices as (
    select distinct
        c.joid as joid,
        s.patid::varchar as patid,
        date(s.svc_date) as service_date,
        s.provider_id,
        s.svc_code as service_code,
        fix_char(s.service_description) as service_desc,
        fix_char(s.sourcesystem) as source_system
    from raw.jocojcmhcservices s
    left outer join clean.jocojococlient c
        on c."source" = 'JOCOJCMHCDEMOGRAPHICS.PATID'
        and c.sourceid = s.patid::varchar
);
