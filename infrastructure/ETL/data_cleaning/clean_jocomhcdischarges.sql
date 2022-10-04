/*
Create clean table for jocomhcdischarges (Johnson county MHC discharges).

Changes:
- Convert patid to varchar for ease of table joins
- Turn char values to upper case for standardization
- Create new program category column from program
- Create new discharge category column from discharge reason
*/

set role :role;

drop table if exists clean.jocojcmhcdischarges cascade;

create table clean.jocojcmhcdischarges as (
    with clean_mhc_discharges as (
        select
            c.joid as joid,
            d.patid::varchar as patid,
            fix_char(d.program_description) as program_desc,
            date(d.admit_date) as admit_date,
            case
                when date(d.dschg_date) < date(d.admit_date) then date(d.admit_date)
                else date(d.dschg_date)
            end as dschg_date,
            fix_char(d.discharge_reason) as dschg_reason,
            fix_char(d.sourcesystem) as source_system
        from raw.jocojcmhcdischarges d
        left outer join clean.jocojococlient c
            on c."source" = 'JOCOJCMHCDEMOGRAPHICS.PATID'
		    and c.sourceid = d.patid::varchar
    )
    select
        joid,
        patid,
        program_desc,
        categorize_mhc_program(program_desc) as program_category,
        admit_date,
        dschg_date,
        dschg_reason,
        categorize_mhc_discharge_reason(dschg_reason) as dschg_category,
        source_system
    from clean_mhc_discharges
);
