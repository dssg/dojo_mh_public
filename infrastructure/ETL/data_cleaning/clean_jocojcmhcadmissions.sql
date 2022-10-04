/*
Create clean table for jocojcmhcadmissions (Johnson county MHC Admissions)

Changes:
- Convert patid to varchar for ease of table joins
- Turn char values to upper case for standardization
- Update admit date = '1914-12-05 00:00:00' to '2014-12-05 00:00:00' (data error)
- Create new program category column from program
*/


set role :role

drop table if exists clean.jocojcmhcadmissions;

create table clean.jocojcmhcadmissions as (
    with mhc_admissions as (
        select distinct
            c.joid as joid,
            a.patid::varchar as patid,
            case when a.admit_date = '1914-12-05 00:00:00' then date('2014-12-05 00:00:00')
            else date(a.admit_date) end as admit_date,
            fix_char(a.program_description) as program_desc,
            fix_char(a.sourcesystem) as source_system
		from raw.jocojcmhcadmissions a
        left outer join clean.jocojococlient c
            on c."source" = 'JOCOJCMHCDEMOGRAPHICS.PATID'
		    and c.sourceid = a.patid::varchar
    )
    select
        joid,
        patid,
        admit_date,
        program_desc,
        categorize_mhc_program(program_desc) as program_category,
        source_system
	from mhc_admissions
);
