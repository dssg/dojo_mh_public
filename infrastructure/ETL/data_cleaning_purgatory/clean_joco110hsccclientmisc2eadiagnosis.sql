--------------------------------------------------------------------------------------------
-- Cleans LMH data from Douglas county, stores it in schema 'clean'
-- Tables: joco110hsccclientmisc2eadiagnosis
--------------------------------------------------------------------------------------------
set role :role;

drop table if exists clean.joco110hsccclientmisc2eadiagnosis;


-- The columns which have Diagnosis X where X \in {1, ..., 106} seem irrelevant
-- (X = 1 for two entries does not in fact mean the same diagnosis!)

-- The columns which have Diagnosis X where X \in {1, ..., 106} seem irrelevant
-- (X = 1 for two entries does not in fact mean the same diagnosis!)
create table clean.joco110hsccclientmisc2eadiagnosis (
  joid int,
  clientid char(25),
  clientmiscid char(25),
  admission_date date,
  primarydiagnosis varchar,
  secondarydiagnosis varchar,
  primarydiagnosis_ICD10 varchar,
  secondarydiagnosis_ICD10 varchar,
  primary_diagnosis_classification varchar,
  secondary_diagnosis_classification varchar,
  suicide_attempt_flag boolean,
  suicidal_flag boolean,
  drug_flag boolean,
  alcohol_flag boolean,
  drug_poisoning_flag boolean,
  other_mental_crisis_flag boolean
);

insert into clean.joco110hsccclientmisc2eadiagnosis
with temporary_diagnosis as
(
	select
	    client.joid,
	    clientid,
	    clientmiscid,
	    -- Extract admission date from 'intro' column
	    to_date(substring(fix_char(intro), 5, 10), 'MM/DD/YYYY')::date as admission_date,
	    -- Turn 'NONE' to null
	    case
	    	when fix_varchar(primarydiagnosis) = 'NONE' then null else
	    	fix_varchar(primarydiagnosis)
	    end as primarydiagnosis,
	    case
	    	when fix_varchar(secondarydiagnosis) = 'NONE' then null else
	    	fix_varchar(secondarydiagnosis)
	    end as secondarydiagnosis,
	    regexp_match(primarydiagnosis, '\(([^\)]+)\)$') as primarydiagnosis_icd10,
	    regexp_match(secondarydiagnosis, '\(([^\)]+)\)$') as secondarydiagnosis_icd10,
	    -- Classify the primary and secondary diagnoses
	    classify_diagnosis(fix_varchar(primarydiagnosis)) as primary_diagnosis_classification,
	    classify_diagnosis(fix_varchar(secondarydiagnosis)) as secondary_diagnosis_classification,
	    case
	        when primarydiagnosis ilike 'suicide attempt%'
	        or secondarydiagnosis ilike 'suicide attempt%' then true else false
	        end as suicide_attempt_flag,
	    case
	        when primarydiagnosis ilike 'suicide attempt%'
	        or secondarydiagnosis ilike 'suicide attempt%'
            or primarydiagnosis ilike 'nonsuicidal self-harm%'
	        or secondarydiagnosis ilike 'nonsuicidal self-harm%'
	        or primarydiagnosis ilike 'suic%ideation%'
	        or secondarydiagnosis ilike 'suic%ideation%'
	        or primarydiagnosis ilike '%self%harm%'
	        or secondarydiagnosis ilike '%self%harm%'
	        or secondarydiagnosis ilike 'PERSONAL HISTORY OF SUICIDAL BEHAVIOR' then true else false
	        end as suicidal_flag,
	    case
	        when primarydiagnosis ilike '%poisoning%self%harm%'
	        or secondarydiagnosis ilike '%poisoning%self%harm%'
	        or primarydiagnosis ilike '%toxic%self%harm%'
	        or secondarydiagnosis ilike '%toxic%self%harm%'
	        or primarydiagnosis ilike '%cocaine%'
	        or secondarydiagnosis ilike '%cocaine%'
	        or primarydiagnosis ilike '%fentanyl%'
	        or secondarydiagnosis ilike '%fentanyl%'
	        or primarydiagnosis ilike '%opioid%'
	        or secondarydiagnosis ilike '%opioid%'
	        or primarydiagnosis ilike '%amphetam%'
	        or secondarydiagnosis ilike '%amphetam%'
	        or primarydiagnosis ilike '%substance abuse%'
	        or secondarydiagnosis ilike '%substance abuse%'
	        or primarydiagnosis ilike '%alcohol %'
	        or secondarydiagnosis ilike '%alcohol %'
	        or primarydiagnosis ilike '%withdrawal%'
	        or secondarydiagnosis ilike '%withdrawal%'
	        or primarydiagnosis ilike '%toxic%ethanol%'
	        or secondarydiagnosis ilike '%toxic%ethanol%' then true else false
	        end as drug_flag,
	    case when primarydiagnosis ilike '%alcohol %'
	        or secondarydiagnosis ilike '%alcohol %'
	        or primarydiagnosis ilike '%toxic%ethanol%'
	        or secondarydiagnosis ilike '%toxic%ethanol%' then true else false
	        end as alcohol_flag,
	    case when primarydiagnosis ilike '%poisoning by%'
	        or secondarydiagnosis ilike '%poisoning by%' then true else false
	        end as drug_poisoning_flag
	from raw.joco110hsccclientmisc2eadiagnosis diag
	left join clean.jocojococlient client
	on client.sourceid::int = diag.clientid::int
	and client.source = 'JOCO110HSCCCLIENT2.CLIENTID'
)
select *,
case when primary_diagnosis_classification in ('PSYCHOTIC', 'BIPOLAR', 'DEPRESSIVE', 'ANXIETY', 'OBSESSIVE-COMPULSIVE', 'TRAUMA- OR STRESSOR- RELATED')
		or secondary_diagnosis_classification in ('PSYCHOTIC', 'BIPOLAR', 'DEPRESSIVE', 'ANXIETY', 'OBSESSIVE-COMPULSIVE', 'TRAUMA- OR STRESSOR- RELATED') then true else false
	    end as other_mental_crisis_flag
from temporary_diagnosis


-- regexp_match returns an array that is formatted as a char (which includes whitespace)
-- here I fix this by removing the curly braces and calling fix_char
update clean.joco110hsccclientmisc2eadiagnosis
set primarydiagnosis_icd10 = replace(replace(primarydiagnosis_icd10, '{', ''), '}', ''),
    secondarydiagnosis_icd10 = replace(replace(secondarydiagnosis_icd10, '{', ''), '}', '');


-- Now that we have created a dedicated column for the ICD-10 codes
-- Remove them from the primary and secondary diagnoses fields
update clean.joco110hsccclientmisc2eadiagnosis
set primarydiagnosis = regexp_replace(primarydiagnosis, '\(([^\)]+)\)$', ''),
    secondarydiagnosis = regexp_replace(secondarydiagnosis, '\(([^\)]+)\)$', '');
