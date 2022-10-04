/*

Create semi_clean tables for JIMS data:
- Add joid
- Add event_date column with most relevant date/null if not available
- Create clean columns for race, sex, dob when relevant
*/


set role :role;


drop table if exists semi_clean.jocojimscasecharges;
create table semi_clean.jocojimscasecharges as
select
	c.joid,
	fix_date(case
		when j.sentencd_dt_32 is not null and j.sentencd_dt_32 <> '' then j.sentencd_dt_32
		else j.ori_occur_23
	end
	) as event_date,
    j.*
from raw.jocojimscasecharges j
left join raw.jocojimscasedata j2
	on j.hash_case_no_0 = j2.hash_case_no_0
left join clean.jocojococlient c
	on j2.mni_id_72 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimscasedata;
create table semi_clean.jocojimscasedata as
select
	c.joid,
	date(j.update_date) as event_date,
	fix_sex(j.sex_7) as sex,
	fix_race(j.race_6) as race,
	fix_date(j.dob_5) as dob,
    j.*
from raw.jocojimscasedata j
left join clean.jocojococlient c
	on j.mni_id_72 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';

/*
--- Removing from semi_clean since court date is in the future
drop table if exists semi_clean.jocojimscasehearings;
create table semi_clean.jocojimscasehearings as
select
	c.joid,
	null as event_date,
	fix_date(j.court_date_73) as event_date, --date in the future
    j.*
from raw.jocojimscasehearings j
left join raw.jocojimscasedata j2
	on j.hash_case_no_0 = j2.hash_case_no_0
left join clean.jocojococlient c
	on j2.mni_id_72 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';
*/


drop table if exists semi_clean.jocojimscasesaddlpaid;
create table semi_clean.jocojimscasesaddlpaid as
select
	c.joid,
	case
		when j.addl_cost_date_152 is not null then fix_date(j.addl_cost_date_152)
		when j.addl_due_dt_226 is not null then fix_date(j.addl_due_dt_226)
		else null
	end as event_date, --- quite a few nulls
    j.*
from raw.jocojimscasesaddlpaid j
left join raw.jocojimscasedata j2
	on j.hash_case_no_0 = j2.hash_case_no_0
left join clean.jocojococlient c
	on j2.mni_id_72 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimscasesbond;
create table semi_clean.jocojimscasesbond as
select
	c.joid,
	date(j2.update_date) as event_date, --- date from casedata
    j.*
from raw.jocojimscasesbond j
left join raw.jocojimscasedata j2
	on j.hash_case_no_0 = j2.hash_case_no_0
left join clean.jocojococlient c
	on j2.mni_id_72 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimscasesbondconditions;
create table semi_clean.jocojimscasesbondconditions as
select
	c.joid,
	case
		when j.bond_dt_113 is not null then fix_date(j.bond_dt_113)
		else date(j2.update_date)
	end as event_date, --- a lot of null dates...
    j.*
from raw.jocojimscasesbondconditions j
left join raw.jocojimscasedata j2
	on j.hash_case_no_0 = j2.hash_case_no_0
left join clean.jocojococlient c
	on j2.mni_id_72 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimscasespaid;
create table semi_clean.jocojimscasespaid as
select
	c.joid,
	date(j2.update_date) as event_date, --- date from casedata
    j.*
from raw.jocojimscasespaid j
left join raw.jocojimscasedata j2
	on j.hash_case_no_0 = j2.hash_case_no_0
left join clean.jocojococlient c
	on j2.mni_id_72 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsinmatecharges;
create table semi_clean.jocojimsinmatecharges as
select
	c.joid,
	fix_date(j.added_date_1) as event_date,
    j.*
from raw.jocojimsinmatecharges j
left join raw.jocojimsinmatedata j2
	on j.hash_booking_no_0 = j2.hash_booking_no_0
left join clean.jocojococlient c
	on j2.mni_58 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsinmatedata;
create table semi_clean.jocojimsinmatedata as
select
	c.joid,
	fix_date(j.bk_dt_40) as event_date,
	fix_sex(j.sex_8) as sex,
	fix_race(j.race_9) as race,
	fix_date(j.int_dob_6) as dob,
	case
		when ethnic_101='Y' then true
		when ethnic_101 ='N' then false
		else NULL
	end as ethnicity,
    j.*
from raw.jocojimsinmatedata j
left join clean.jocojococlient c
	on j.mni_58 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsinmatepov;
create table semi_clean.jocojimsinmatepov as
select
	c.joid,
	fix_date(j2.bk_dt_40) as event_date, --- date taken from inmatedata
    j.*
from raw.jocojimsinmatepov j
left join raw.jocojimsinmatedata j2
	on j.hash_booking_no_0 = j2.hash_booking_no_0
left join clean.jocojococlient c
	on j2.mni_58 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsinmatescars;
create table semi_clean.jocojimsinmatescars as
select
	c.joid,
	fix_date(j2.bk_dt_40) as event_date, --- date taken from inmatedata
    j.*
from raw.jocojimsinmatescars j
left join raw.jocojimsinmatedata j2
	on j.hash_booking_no_0 = j2.hash_booking_no_0
left join clean.jocojococlient c
	on j2.mni_58 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


set role :role;
drop table if exists semi_clean.jocojimsjuvinmate;
create table semi_clean.jocojimsjuvinmate as
select
	c.joid,
	fix_date(j.bk_dt_40) as event_date,
	fix_sex(j.s_e_x_8) as sex,
	fix_race(j.race_9) as race,
	fix_date(j.dob_6) as dob,
	case
		when j.hisp_120 = 'Y' then true
		when j.hisp_120 = 'N' then false
		else null
	end as ethnicity,
    j.*
from raw.jocojimsjuvinmate j
left join raw.jocojimsjuvinmatedata j2
	on j.hash_booking_no_0 = j2.hash_booking_no_0
left join clean.jocojococlient c
	on j2.mni_58 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsjuvinmatecharges;
create table semi_clean.jocojimsjuvinmatecharges as
select
	c.joid,
	fix_date(j.added_date_1) as event_date, --- is this the best date? also sentence, release dates etc.
    j.*
from raw.jocojimsjuvinmatecharges j
left join raw.jocojimsjuvinmatedata j2
	on j.hash_booking_no_0 = j2.hash_booking_no_0
left join clean.jocojococlient c
	on j2.mni_58 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsjuvinmatedata;
create table semi_clean.jocojimsjuvinmatedata as
select
	c.joid,
	fix_date(j.bk_dt_40) as event_date, --- bk_data_40 or arrest_dt_34?
	fix_sex(j.s_e_x_8) as sex,
	fix_race(j.race_9) as race,
	fix_date(j.dob_6) as dob,
	case
		when hisp_120 = 'Y' then true
		when hisp_120 = 'N' then false
		else null
	end as ethnicity,
    j.*
from raw.jocojimsjuvinmatedata j
left join clean.jocojococlient c
	on j.mni_58 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsjuvinmatepov;
create table semi_clean.jocojimsjuvinmatepov as
select
	c.joid,
	fix_date(j2.bk_dt_40) as event_date, --- date taken from juvinmatedata
    j.*
from raw.jocojimsjuvinmatepov j
left join raw.jocojimsjuvinmatedata j2
	on j.hash_booking_no_0 = j2.hash_booking_no_0
left join clean.jocojococlient c
	on j2.mni_58 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsjuvinmatescars;
create table semi_clean.jocojimsjuvinmatescars as
select
	c.joid,
	fix_date(j2.bk_dt_40) as event_date, --- date taken from juvinmatedata
    j.*
from raw.jocojimsjuvinmatescars j
left join raw.jocojimsjuvinmatedata j2
	on j.hash_booking_no_0 = j2.hash_booking_no_0
left join clean.jocojococlient c
	on j2.mni_58 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimslsirdata;
create table semi_clean.jocojimslsirdata as
select
	c.joid,
	j.create_date::date as event_date,
	j.*
from raw.jocojimslsirdata j
left join raw.jocojimscasedata j2
	on j.hash_case_no_71_1 = j2.hash_case_no_0
left join clean.jocojococlient c
	on j2.mni_id_72 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


--- (jocojocojimsmnhdata & jocojimsmnh_attempts in clean schema)


drop table if exists semi_clean.jocojimsmnibooking;
create table semi_clean.jocojimsmnibooking as
select
	c.joid,
	fix_date(null) as event_date, --- no date?
    j.*
from raw.jocojimsmnibooking j
left join clean.jocojococlient c
	on j.mni_no_0 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsmnicasenos;
create table semi_clean.jocojimsmnicasenos as
select
	c.joid,
	fix_date(null) as event_date, --- no date?
    j.*
from raw.jocojimsmnicasenos j
left join clean.jocojococlient c
	on j.mni_no_0  = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsmnicitations;
create table semi_clean.jocojimsmnicitations as
select
	c.joid,
	fix_date(null) as event_date, --- no date?
    j.*
from raw.jocojimsmnicitations j
left join clean.jocojococlient c
	on j.mni_no_0 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsmnicomments;
create table semi_clean.jocojimsmnicomments as
select
	c.joid,
	fix_date(null) as event_date, --- no date?
    j.*
from raw.jocojimsmnicomments j
left join clean.jocojococlient c
	on j.mni_no_0  = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsmnicriminal;
create table semi_clean.jocojimsmnicriminal as
select
	c.joid,
	fix_date(null) as event_date, --- no date?
    j.*
from raw.jocojimsmnicriminal j
left join clean.jocojococlient c
	on j.mni_no_0 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsmniempaddr;
create table semi_clean.jocojimsmniempaddr as
select
	c.joid,
	fix_date(j.enter_dt_46) as event_date,
    j.*
from raw.jocojimsmniempaddr j
left join clean.jocojococlient c
	on j.mni_no_0 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsmnifir;
create table semi_clean.jocojimsmnifir as
select
	c.joid,
	fix_date(null) as event_date, --- no date?
    j.*
from raw.jocojimsmnifir j
left join clean.jocojococlient c
	on j.mni_no_0 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsmniprobnos;
create table semi_clean.jocojimsmniprobnos as
select
	c.joid,
	fix_date(null) as event_date, --- no date?
    j.*
from raw.jocojimsmniprobnos j
left join clean.jocojococlient c
	on j.mni_no_0 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsmniprosnos;
create table semi_clean.jocojimsmniprosnos as
select
	c.joid,
	fix_date(null) as event_date, --- no date?
    j.*
from raw.jocojimsmniprosnos j
left join clean.jocojococlient c
	on j.mni_no_0 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsmniresaddr;
create table semi_clean.jocojimsmniresaddr as
select
	c.joid,
	fix_date(j.enter_date_44) as event_date,
    j.*
from raw.jocojimsmniresaddr j
left join clean.jocojococlient c
	on j.mni_no_0 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsmnisummons;
create table semi_clean.jocojimsmnisummons as
select
	c.joid,
	fix_date(j2.vio_date_44) as event_date, --- review date, taken from summonsdata
    j.*
from raw.jocojimsmnisummons j
left join raw.jocojimssummonsdata j2
	on j.summon_55 = j2.summons_no_0
left join clean.jocojococlient c
	on j.mni_no_0 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsmnitraffic;
create table semi_clean.jocojimsmnitraffic as
select
	c.joid,
	fix_date(null) as event_date, --- no dates?
    j.*
from raw.jocojimsmnitraffic j
left join clean.jocojococlient c
	on j.mni_no_0 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsnameindex;
create table semi_clean.jocojimsnameindex as
select
	c.joid,
	fix_date(null) as event_date, --- DATE MISSING!
	fix_sex(j.sex_7) as sex,
	fix_race(j.race_24) as race,
	fix_date(j.dob_8) as dob,
	case
		when ethnic_105 = 'Y' then true
		when ethnic_105 = 'N' then false
		else null
	end as ethnicity,
    j.*
from raw.jocojimsnameindex j
left join clean.jocojococlient c
	on j.mni_no_0 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimspretrialassessdata;
create table semi_clean.jocojimspretrialassessdata as
select
	c.joid,
	fix_date(j.create_date::date::varchar) as event_date,
	j.*
from raw.jocojimspretrialassessdata j
left join raw.jocojimsinmatedata j2
	on j.hash_pta_no_0  = j2.hash_booking_no_0
left join clean.jocojococlient c
	on j2.mni_58  = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';



drop table if exists semi_clean.jocojimsprobationdata;
create table semi_clean.jocojimsprobationdata as
select
	c.joid,
	/*
	fix_date(case
		when j.prob_due_dt_16 is not null and j.prob_due_dt_16 <> '' then j.prob_due_dt_16
		when j.appointment_36 is not null and j.appointment_36 <> '' then j.appointment_36
		when j.begin_date_117 is not null and j.begin_date_117 <> '' then j.begin_date_117
		else null
	end) as event_date,
	*/ --date in the future
	null as event_date,
	case
		when ethn_166 = 'Y' then true
		when ethn_166 = 'N' then false
		else null
	end as ethnicity,
    j.*
from raw.jocojimsprobationdata j
left join clean.jocojococlient c
	on j.mni_2 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsprobcustdays;
create table semi_clean.jocojimsprobcustdays as
select
	c.joid,
	fix_date(j.arrest_dt_153) as event_date,
    j.*
from raw.jocojimsprobcustdays j
left join raw.jocojimsprobationdata j2
	on j.prob_no_0  = j2.prob_no_0
left join clean.jocojococlient c
	on j2.mni_2  = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsprobdrugtest;
create table semi_clean.jocojimsprobdrugtest as
select
	c.joid,
	fix_date(j.drug_test_date_120) as event_date,
    j.*
from raw.jocojimsprobdrugtest j
left join raw.jocojimsprobationdata j2
	on j.prob_no_0  = j2.prob_no_0
left join clean.jocojococlient c
	on j2.mni_2  = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';

/*
--- Removing it from semi clean since date is in the future
drop table if exists semi_clean.jocojimsprobstatus;
create table semi_clean.jocojimsprobstatus as
select
	c.joid,
	fix_date(j.stat_dt_35) as event_date,
    j.*
from raw.jocojimsprobstatus j
left join raw.jocojimsprobationdata j2
	on j.prob_no_0  = j2.prob_no_0
left join clean.jocojococlient c
	on j2.mni_2  = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';
*/


/*
--- Removing from semi clean since date is in the future
drop table if exists semi_clean.jocojimsprobtype;
create table semi_clean.jocojimsprobtype as
select
	c.joid,
	fix_date(case
		when j.prob_end_dt_86 is not null and j.prob_end_dt_86 <> '' then j.prob_end_dt_86
		else j.probation_due_date_85 end) as event_date,
    j.*
from raw.jocojimsprobtype j
left join raw.jocojimsprobationdata j2
	on j.prob_no_0  = j2.prob_no_0
left join clean.jocojococlient c
	on j2.mni_2  = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';
*/


drop table if exists semi_clean.jocojimsproscharges;
create table semi_clean.jocojimsproscharges as
select
	c.joid,
	fix_date(j.occur_date_29) as event_date,
    j.*
from raw.jocojimsproscharges j
left join raw.jocojimsprosdefdata j2
	on j.prdef_no_0  = j2.prdef_no_0
left join clean.jocojococlient c
	on j2.mni_id_35 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsprosdata;
create table semi_clean.jocojimsprosdata as
select
	c.joid,
	fix_date(j.occur_date_5) as event_date,
	j.*
from raw.jocojimsprosdata j
left join raw.jocojimsprosdefdata j2
	on j.pros_no_0 = substring(j2.prdef_no_0, 1, 10)
left join clean.jocojococlient c
	on j2.mni_id_35 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimsprosdefdata;
create table semi_clean.jocojimsprosdefdata as
select
	c.joid,
	fix_date(j.court_dt_43) as event_date,
	fix_sex(j.sex_7) as sex,
	fix_race(j.rc_6) as race,
	fix_date(j.dob_5) as dob,
	case
		when ethnic_code_66 = 'Y' then true
		when ethnic_code_66 = 'N' then false
		else null
	end as ethnicity,
    j.*
from raw.jocojimsprosdefdata j
left join clean.jocojococlient c
	on j.mni_id_35 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


--- jocojimssectioncodes not relevant for semi_clean: it translates charge codes into a text descriptions, e.g. jocojimsinmatecharges.


drop table if exists semi_clean.jocojimssummonsdata;
create table semi_clean.jocojimssummonsdata as
select
	c.joid,
	fix_date(j.vio_date_44) as event_date, --- review date column
	fix_sex(j.sex_10) as sex,
	fix_race(j.race_12) as race,
	fix_date(j.dob_7) as dob,
	case
		when ethn_124 = 'Y' then true
		when ethn_124 = 'N' then false
		else null
	end as ethnicity,
    j.*
from raw.jocojimssummonsdata j
left join clean.jocojococlient c
	on j.mni_key_65  = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimssummonsviolations;
create table semi_clean.jocojimssummonsviolations as
select
	c.joid,
	fix_date(j2.vio_date_44) as event_date, ---review date column, taken from summonsdata
    j.*
from raw.jocojimssummonsviolations j
left join raw.jocojimssummonsdata j2
	on j.summons_no_0  = j2.summons_no_0
left join clean.jocojococlient c
	on j2.mni_key_65  = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimswarranthstdata;
create table semi_clean.jocojimswarranthstdata as
select
	c.joid,
	fix_date(case when j.issue_dt_13 = '' then null else j.issue_dt_13 end) as event_date,
	fix_sex(j.sex_82) as sex,
	fix_race(j.race_83) as race,
	fix_date(case when j.dob_84 = '' then null else j.issue_dt_13 end) as dob,
    j.*
from raw.jocojimswarranthstdata j
left join clean.jocojococlient c
	on j.mni_2  = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimswarranthstlog;
create table semi_clean.jocojimswarranthstlog as
select
	c.joid,
	fix_date(j.war_log_dt_133) as event_date,
    j.*
from raw.jocojimswarranthstlog j
left join raw.jocojimswarrantmstdata j2
	on j.warrant_no_0 = j2.warrant_no_0
left join clean.jocojococlient c
	on j2.mni_2  = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimswarrantmstdata;
create table semi_clean.jocojimswarrantmstdata as
select
	c.joid,
	fix_date(j.issue_dt_13) as event_date,
	fix_sex(j.sex_82) as sex,
	fix_race(j.race_83) as race,
	fix_date(j.dob_84) as dob,
    j.*
from raw.jocojimswarrantmstdata j
left join clean.jocojococlient c
	on j.mni_2  = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


drop table if exists semi_clean.jocojimswarrantmstlog;
create table semi_clean.jocojimswarrantmstlog as
select
	c.joid,
	fix_date(j.war_log_dt_133) as event_date,
    j.*
from raw.jocojimswarrantmstlog j
left join raw.jocojimswarrantmstdata j2
	on j.warrant_no_0 = j2.warrant_no_0
left join clean.jocojococlient c
	on j2.mni_2  = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';


/*
--- Join currently not working as hash_yls_no_0 is missing
drop table if exists semi_clean.jocojimsylsdata;
create table semi_clean.jocojimsylsdata as
select
	c.joid,
	fix_date(j.assessment_date_91) as event_date,
	j.*
from raw.jocojimsylsdata j
left join raw.jocojimsinmatedata j2
	on j.hash_yls_no_0 = j2.hash_booking_no_0
left join clean.jocojococlient c
	on j2.mni_58 = c.sourceid
	and c.source = 'JOCOJIMSNAMEINDEX.MNI_NO_0';
*/
