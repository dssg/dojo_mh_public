--------------------------------------------------------------------------------------------
-- Cleans medical examiner data from Johnson and Douglas county, stores it in schema 'clean'
-- Tables: jocojcmexoverdosessuicides, jocodcmexoverdosessuicides
--------------------------------------------------------------------------------------------
set role :role;


----------------------
-- Johnson county data
----------------------
drop table if exists clean.jocojcmexoverdosessuicides;

create table clean.jocojcmexoverdosessuicides (
  joid int,
  id varchar,
  dateofbirth date,
  dateofdeath date,
  age integer,
  mannerofdeath varchar,
  mechanism varchar,
  typeofdrugs varchar,
  overdosed boolean,
  suicide boolean
);

-- Update column 'overdosed' to be true whenever mechanism = 'Overdose'
-- There is a column 'Poisoning' but it is probably better to trust the hospital
-- in distinguishing between overdose and poisoning
insert into clean.jocojcmexoverdosessuicides
select
  client.joid as joid,
  mex.id as id,
  time_to_date(mex.dob) as dateofbirth,
  time_to_date(dod) as dateofdeath,
  extract(year from time_to_date(dod)) - extract(year from time_to_date(mex.dob)) as age,
  fix_varchar(mannerofdeath) as mannerofdeath,
  fix_varchar(mechanism) as mechanism,
  fix_varchar(typeofdrugs) as typeofdrugs,
  case
  	when mechanism ilike '%overdose%' then true
  	else false
  end as overdosed,
  case
	when mannerofdeath ilike '%sui%' then true
  	else false
  end as suicide
from raw.jocojcmexoverdosessuicides mex
left join clean.jocojococlient client
on client.sourceid = mex.id
and client.source = 'JOCOJCMEXOVERDOSESSUICIDES.ID';


----------------------
-- Douglas county data
----------------------
drop table if exists clean.jocodcmexoverdosessuicides;

create table clean.jocodcmexoverdosessuicides (
  joid int,
  casenum varchar,
  sex varchar,
  dateofbirth date,
  dateofdeath date,
  age integer,
  mannerofdeath varchar,
  causeofdeath1 varchar,
  causeofdeath2 varchar,
  otherconditions varchar,
  overdosed boolean default false,
  suicide boolean default false
);


-- Add column 'suicide'
-- Add column 'overdosed' to be true whenever cause of death was intoxication (ilike is for case insensitive matching)
-- Use shortened string 'into' because of postgresql 63 char limit cutting of fields
-- Match 'overdose' as well as 'fentanyl' (1 case where char limit removes even intoxication but fentanyl remains)
insert into clean.jocodcmexoverdosessuicides
select
  client.joid as joid,
  fix_char(casenum) as casenum,
  fix_sex(gender) as sex,
  time_to_date(mex.dob) as dateofbirth,
  time_to_date(dod) as dateofdeath,
  replace(age, ' Years', '')::integer as age,
  fix_varchar(mannerofdeath) as mannerofdeath,
  fix_varchar(cod1) as causeofdeath1,
  fix_varchar(cod2) as causeofdeath2,
  fix_varchar(otherconditions) as otherconditions,
  case
  	when
  	(fix_varchar(cod1) ilike '%into%' or fix_varchar(cod1) ilike '%overdose%' or fix_varchar(cod1) ilike '%fentanyl%') or
    (fix_varchar(cod2) ilike '%into%' or fix_varchar(cod2) ilike '%overdose%' or fix_varchar(cod2) ilike '%fentanyl%') then true
  	else false
  end as overdosed,
  case
	when mannerofdeath ilike '%sui%' then true
  	else false
  end as suicide
from raw.jocodcmexoverdosessuicides mex
left join clean.jocojococlient client
on client.sourceid = mex.casenum
and client.source = 'JOCODCMEXOVERDOSESSUICIDES.CASENUM';
