-- Semi cleans tables that have demographics data but no as of date
-- Useful for the fairness audit
set role :role;

drop table if exists semi_clean.joco110hsccclient2;
drop table if exists semi_clean.jocokdocdemographics;

create table semi_clean.joco110hsccclient2 as (
    select
        c.joid as joid,
        fix_sex(t.clientsex) as sex,
        fix_race(t.clientrace) as race,
        null as event_date,
        t.*
        from raw.joco110hsccclient2 t
        left join clean.jocojococlient c
       	    on c."source" = 'JOCO110HSCCCLIENT2.CLIENTID'
		    and c.sourceid = clientid::varchar
);

create table semi_clean.jocokdocdemographics as (
    select
        c.joid as joid,
        null as event_date,
        fix_sex(t.gender) as sex,
        fix_race(t.race) as race,
        case
        	when ethnicity = 'H' then true
        	when ethnicity = 'N' then false
        	else null
        end as ethnicity
        from raw.jocokdocdemographics t
        left join clean.jocojococlient c
       	    on c."source" = 'JOCOKDOCDEMOGRAPHICS.DOCNUM'
		    and c.hash_sourceid = hash_docnum
);
