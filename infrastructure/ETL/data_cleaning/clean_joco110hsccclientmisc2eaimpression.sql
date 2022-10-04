/*
 * creates a new table in clean schema that:
 * selects the relevant columns
 * cleans strings, time, etc
 * makes a flag for suicide, drugs, and other mental health crises
 * adds joid
 */



set role :role;

drop table if exists clean.joco110hsccclientmisc2eaimpression;

create table clean.joco110hsccclientmisc2eaimpression as
select
    	joid,
    	clientmiscid,
		fieldnamenum as "impression_num",
		to_date(substring(intro, 5,18), 'MM/DD/YYYY HH:MI AM WITHOUT TIME ZONE')::date as incident_date,
		to_timestamp(substring(intro, 5,18), 'MM/DD/YYYY HH:MI AM WITHOUT TIME ZONE')::timestamp as incident_time,
		fix_char(primaryimpression) as primary_impression,
		fix_char(secondaryimpression) as secondary_impression,
		fix_char(chiefcomplaint) as chief_complaint,
		cast(clientid as varchar) as clientid,
		case when (chiefcomplaint ilike '%sui_id%'
			or  primaryimpression ilike '%sui_id%'
			or secondaryimpression ilike '%sui_id%'
			or chiefcomplaint ilike '%kill%self%'
			or chiefcomplaint ilike '%self%harm%'
			or chiefcomplaint ilike '%i %want% die%'
	  		or chiefcomplaint ilike '%i %dont% want% live%'
	  		or chiefcomplaint ilike '%cut%wrists%')
			and (chiefcomplaint not ilike '%don%want%die%') then true
			when primaryimpression ilike '%psych%'
			and chiefcomplaint ilike '%end%life%' then true
			else false end as suicidal_flag,
		  case when primaryimpression ilike 'suicide attempt'
		  or secondaryimpression ilike 'suicide attempt' then true
		  else false end as suicide_attempt_flag,
		  case when  primaryimpression ilike '%subst%abuse%'
			or secondaryimpression ilike '%subst%abuse%'
			or chiefcomplaint ilike '%subst%abuse%' then true
			when primaryimpression ilike '%overdose%'
			or secondaryimpression ilike '%overdose%'
			or chiefcomplaint ilike '%overdose%' then true
			when primaryimpression ilike '%ETOH%'
			or secondaryimpression ilike '%ETOH%'
			or chiefcomplaint ilike '%ETOH%' then true
			when primaryimpression ilike '%alcohol%'
			or secondaryimpression ilike '%alcohol%'
			or chiefcomplaint ilike '%alcohol%'then true
			when chiefcomplaint ilike '% meth' then true
			when chiefcomplaint ilike 'meth %' then true
			when chiefcomplaint ilike '% meth %' then true
			when chiefcomplaint ilike '% meth.%' then true
			when chiefcomplaint ilike '% heroin%' then true
			when ( primaryimpression ilike '%drug abuse%'
			or secondaryimpression ilike '%drug abuse%'
			or chiefcomplaint ilike '%drug abuse%')
			and (chiefcomplaint not ilike '%laundry%'
			and chiefcomplaint not ilike '%detergent%'
			and chiefcomplaint not ilike '%tide%') then true
			else false end as drug_flag,
		case when primaryimpression ilike '%psych%'
			or secondaryimpression ilike '%psych%'
			or chiefcomplaint ilike '%psych%'
			or primaryimpression ilike '%anxiety%'
			or secondaryimpression ilike '%anxiety%'
			or chiefcomplaint ilike '%anxiety%'
			or primaryimpression ilike '%depress%'
			or secondaryimpression ilike '%depress%'
			or chiefcomplaint ilike '%depress%' then true
			else false end as other_mental_crisis_flag,
		case when primaryimpression ilike '%alcohol%'
			or secondaryimpression ilike '%alcohol%'
			or chiefcomplaint ilike '%alcohol%' then true
			when primaryimpression ilike '%ETOH%'
			or secondaryimpression ilike '%ETOH%'
			or chiefcomplaint ilike '%ETOH%' then true
			else false end as alcohol_flag,
        case when  (primaryimpression ilike '%drug%ingest%'
			or secondaryimpression ilike '%drug%ingest%'
			or chiefcomplaint ilike '%drug%ingest%')
			and (primaryimpression not ilike '%allerg%'
			and secondaryimpression not ilike '%allerg%' )
			then true
			else false end as drug_poisoning_flag
		from raw.joco110hsccclientmisc2eaimpression i
		left outer join clean.jocojococlient c
		on c."source" = 'JOCO110HSCCCLIENT2.CLIENTID'
		and c.sourceid = clientid::varchar;
