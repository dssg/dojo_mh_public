set role :role;

drop table if exists semi_clean.jocojimscasedata;
drop table if exists semi_clean.jocojimsbailhstcases;
drop table if exists semi_clean.jocojimsbailmstcases;
drop table if exists semi_clean.jocojimsbailmstdata;
drop table if exists semi_clean.jocojimsbailhstdata;
drop table if exists semi_clean.jocojimsbailhstbailinfo;
drop table if exists semi_clean.jocojimsbailmstbailinfo;


--- For this table dates are in the future, so we are setting them to
--- null so that client events is not using it. We want this table in semi_clean
--- because of the demographics data for the fairness audit.
create table semi_clean.jocojimsbailhstdata as (
    select
        c.joid as joid,
        fix_date(t.dob_7) as dob,
        fix_sex(t.sex_9) as sex,
        fix_race(t.rc_8) as race,
        -- date in the future
        --fix_date(t.arrange_dt_29) as event_date,
        null as event_date,
        t.*
        from raw.jocojimsbailhstdata t
        left join clean.jocojococlient c
       	    on c."source" = 'JOCOJIMSNAMEINDEX.MNI_NO_0'
		    and c.sourceid = t.mni_id_36::varchar
        where joid is not null -- somehow there are three null joids in here  — remove them!
);

--- For this table dates are in the future, so we are setting them to
--- null so that client events is not using it. We want this table in semi_clean
--- because of the demographics data for the fairness audit.
create table semi_clean.jocojimsbailmstdata as (
    select
        c.joid as joid,
        fix_date(t.dob_7) as dob,
        fix_sex(t.sex_9) as sex,
        fix_race(t.rc_8) as race,
        -- date in the future
        --fix_date(t.arrange_dt_29) as event_date,
        null as event_date,
        t.*
        from raw.jocojimsbailmstdata t
        left join clean.jocojococlient c
        	on c."source" = 'JOCOJIMSNAMEINDEX.MNI_NO_0'
			and c.sourceid = t.mni_id_36::varchar
);


-- This has create_date, update_date, and closed_date -> what are those? which one do we want?
create table semi_clean.jocojimscasedata as (
    select
        c.joid as joid,
        fix_date(t.dob_5) as dob,
        fix_race(t.race_6) as race,
        fix_sex(t.sex_7) as sex,
        fix_date(t.closed_date_125) as event_date,
        t.*
        from raw.jocojimscasedata t
        left join clean.jocojococlient c
            on c."source" = 'JOCOJIMSNAMEINDEX.MNI_NO_0'
		    and c.sourceid = t.mni_id_72::varchar
);


create table semi_clean.jocojimsbailhstcases as (
    select
        c.joid as joid,
        null as event_date,
        --fix_date(b.arrange_dt_29) as event_date, --- date in the future
        t.*
        from raw.jocojimsbailhstcases t
        left join raw.jocojimsbailhstdata b
            on t.bail_no_0 = b.bail_no_0
        left join clean.jocojococlient c
            on c."source" = 'JOCOJIMSNAMEINDEX.MNI_NO_0'
		    and c.sourceid = b.mni_id_36::varchar
);


-- First join with bailmstdata, then with client
-- Has no date column ...
create table semi_clean.jocojimsbailmstcases as (
    select
        c.joid as joid,
        null as event_date,
        --fix_date(b.arrange_dt_29) as event_date, --- date in the future
        t.*
        from raw.jocojimsbailmstcases t
        left join raw.jocojimsbailmstdata b
            on t.bail_no_0 = b.bail_no_0
        left join clean.jocojococlient c
            on c."source" = 'JOCOJIMSNAMEINDEX.MNI_NO_0'
		    and c.sourceid = b.mni_id_36::varchar
);


-- First join with bailmstdata, then with client
create table semi_clean.jocojimsbailmstbailinfo as (
    select
        c.joid as joid,
        fix_date(t.receipt_date_63) as event_date,
        t.*
        from raw.jocojimsbailmstbailinfo t
        left join raw.jocojimsbailmstdata b
            on t.bail_no_0 = b.bail_no_0
        left join clean.jocojococlient c
            on c."source" = 'JOCOJIMSNAMEINDEX.MNI_NO_0'
		    and c.sourceid = b.mni_id_36::varchar
);

-- First join with bailhstdata, then with client
create table semi_clean.jocojimsbailhstbailinfo as (
    select
        c.joid as joid,
        fix_date(t.receipt_date_63) as event_date,
        t.*
        from raw.jocojimsbailhstbailinfo t
        left join raw.jocojimsbailhstdata b
            on t.bail_no_0 = b.bail_no_0
        left join clean.jocojococlient c
            on c."source" = 'JOCOJIMSNAMEINDEX.MNI_NO_0'
		    and c.sourceid = b.mni_id_36::varchar
);
