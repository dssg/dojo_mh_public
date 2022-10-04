/*
Create clean jocojcmhcoutcomes table (Johnson County MHC outcomes).

(Part of this code has been re-used from 2018 Johnson county project.)

Changes:
- Add joid from jocojococlient table
- Change patid to varchar for ease of table joins
- Set dla_score and cafas_score to null when score is 0 (inexistent)
- Turn sourcesystem variable to upper case
*/

set role :role;

drop table if exists clean.jocojcmhcoutcomes cascade;

create table clean.jocojcmhcoutcomes
as
    select c.joid as joid,
           o.patid::varchar as patid,
           date(o.assess_date) as assess_date,
           case
                when o.dla_score = 0 then null
                else o.dla_score::integer
            end as dla_score,
            case
                when o.cafas_score = 0 then null
                else o.cafas_score::integer
            end as cafas_score,
           fix_char(o.sourcesystem) as source_system
    from raw.jocojcmhcoutcomes o
    left outer join clean.jocojococlient c
        on c."source" = 'JOCOJCMHCDEMOGRAPHICS.PATID'
		and c.sourceid = o.patid::varchar;
