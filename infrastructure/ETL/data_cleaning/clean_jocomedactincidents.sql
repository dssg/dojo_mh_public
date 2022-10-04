/*
 * creates a new table in clean schema that:
 * selects the relevant columns
 * cleans strings, time, etc
 * makes a flag for suicide, drugs, and other mental health crises.
 * adds joid
 */

set role :role;

drop table if exists clean.jocomedactincidents;

create table clean.jocomedactincidents as
    select
    	   joid,
    	   m.id as id,
           hash_rcdid,
           encode(hash_clientssn,'hex') as hash_clientssn,
           encode(hash_clientnamefirst,'hex') as hash_fname,
           encode(hash_clientnamemiddle, 'hex') as hash_mname,
           encode(hash_clientnamelast,'hex') as hash_lname,
           clientdateofbirth::date as dob,
           fix_race(clientrace) as race,
           fix_sex(clientsex) as sex,
           encode(hash_clientaddress,'hex') as hash_clientaddress,
           encode(hash_clientaddress2, 'hex') as hash_clientaddress2,
           case
               when homeless = 1 then True
               when homeless is null then False
           end as homeless,
           fix_city(remove_ks(fix_char(clientcity))) as client_city,
           fix_state(fix_char(clientstate)) as client_state,
           fix_zip(clientzip) as client_zip,
           joco_resident(clienttract2010id, encode(hash_clientaddress, 'hex')) as joco_resident,
           encode(hash_calladdress,'hex') as hash_calladdress,
           fix_city(remove_ks(fix_char(callcity))) as call_city,
           fix_state(fix_char(callstate)) as call_state,
           fix_zip(callzip) as call_zip,
           case
               when fix_zip(callzip) = fix_zip(clientzip) then 'same zip'
               else 'not same zip'
           end as samecallandclientzip,
           incidentdate as incident_time,
           incidentdate::date as incident_date,
           dispatchdatetime,
           unitdispatchdatetime,
           unitenroutedatetime,
           unitarrivescenedatetime,
           arrivepatientdatetime,
           unitleftscenedatetime,
           unitarrivedestdatetime,
           fix_char(clienttriage) as triage,
           fix_char(clientpriimpression) as primary_impression,
           fix_char(clientsecimpression) as secondary_impression,
           fix_char(clientchiefcomplaint) as chief_complaint,
           case when (clientchiefcomplaint ilike '%sui_id%'
				or  clientpriimpression ilike '%sui_id%'
				or clientsecimpression ilike '%sui_id%'
				or clientchiefcomplaint ilike '%kill%self%'
				or clientchiefcomplaint ilike '%self%harm%'
				or clientchiefcomplaint ilike '%i %want% die%'
	  			or clientchiefcomplaint ilike '%i %don% want% live%'
	  			or clientchiefcomplaint ilike '%cut%wrists%')
			and (clientchiefcomplaint not ilike '%don%want%die%') then true
				when  clientpriimpression ilike '%psych%'
				and clientchiefcomplaint ilike '%end%life%' then true
				else false end as suicidal_flag,
		   case when clientpriimpression ilike 'suicide attempt'
		   		or clientsecimpression ilike 'suicide attempt' then true
		   		else false end as suicide_attempt_flag,
		   case when  clientpriimpression ilike '%subst%abuse%'
				or clientsecimpression ilike '%subst%abuse%'
				or clientchiefcomplaint ilike '%subst%abuse%' then true
			    when  clientpriimpression ilike '%overdose%'
				or clientsecimpression ilike '%overdose%'
				or clientchiefcomplaint ilike '%overdose%' then true
				when clientpriimpression ilike '%ETOH%'
				or clientsecimpression ilike '%ETOH%'
				or clientchiefcomplaint ilike '%ETOH%' then true
				when  clientpriimpression ilike '%alcohol%'
				or clientsecimpression ilike '%alcohol%'
				or clientchiefcomplaint ilike '%alcohol%'then true
                when clientchiefcomplaint ilike '% meth' then true
                when clientchiefcomplaint ilike 'meth %' then true
                when clientchiefcomplaint ilike '% meth %' then true
                when clientchiefcomplaint ilike '% meth.%' then true
			    when clientchiefcomplaint ilike '% heroin%' then true
			    when ( clientpriimpression ilike '%drug abuse%'
			    or clientsecimpression ilike '%drug abuse%'
			    or clientchiefcomplaint ilike '%drug abuse%')
				and (clientchiefcomplaint not ilike '%laundry%'
				and clientchiefcomplaint not ilike '%detergent%'
				and clientchiefcomplaint not ilike '%tide%') then true
				else false end as drug_flag,
		   case when  clientpriimpression ilike '%psych%'
				or clientsecimpression ilike '%psych%'
				or clientchiefcomplaint ilike '%psych%'
				or  clientpriimpression ilike '%anxiety%'
				or clientsecimpression ilike '%anxiety%'
				or clientchiefcomplaint ilike '%anxiety%'
				or  clientpriimpression ilike '%depress%'
				or clientsecimpression ilike '%depress%'
				or clientchiefcomplaint ilike '%depress%' then true
				else false end as other_mental_crisis_flag,
		   case when  clientpriimpression ilike '%alcohol%'
				or clientsecimpression ilike '%alcohol%'
				or clientchiefcomplaint ilike '%alcohol%' then true
				when clientpriimpression ilike '%ETOH%'
				or clientsecimpression ilike '%ETOH%'
				or clientchiefcomplaint ilike '%ETOH%' then true
			    else false end as alcohol_flag,
            case when  (clientpriimpression ilike '%drug%ingest%'
			    or clientsecimpression ilike '%drug%ingest%'
			    or clientchiefcomplaint ilike '%drug%ingest%')
			    and (clientpriimpression not ilike '%allerg%'
			    and clientsecimpression not ilike '%allerg%' )
			    then true
				else false end as drug_poisoning_flag,
           fix_char(clientprocedures) as client_procedures,
           fix_char(clientmedications) as client_medications,
           fix_char(clientvitals) as client_vitals,
           case
               when fix_char(clientdisposition) = '' then null
               when fix_char(clientdisposition) = 'NOT APPLICABLE' then null
               else fix_char(clientdisposition)
           end as disposition,
           case
               when fix_char(clientdisposition) similar to '%(TRANSPORTED)%' then true
               when clientdisposition is null then null
               else false
           end as transported,
           case
               when fix_char(clientdisposition) similar to '%(TREATED)%' then true
               when clientdisposition is null then null
               else false
           end as treated,
           case
               when fix_char(destinationname) = 'OVERLAND PARK REGIONAL MEDICAL CENT'
                   then 'OVERLAND PARK REGIONAL MEDICAL CENTER'
               when fix_char(destinationname) = 'NOT APPLICABLE' then null
               when fix_char(destinationname) = 'NOT KNOWN' then null
               when fix_char(destinationname) = '' then null
               else fix_char(destinationname)
           end as destination,
           encode(hash_serviceprovidernamefirst,'hex') as hash_serviceprovidernamefirst,
           encode(hash_serviceprovidernamelast, 'hex') as hash_serviceprovidernamelast,
           sourcedate,
           coalesce(
               unitdispatchdatetime,
               unitenroutedatetime,
               unitarrivescenedatetime,
               arrivepatientdatetime,
               unitleftscenedatetime,
               unitarrivedestdatetime
           ) as startdt,
           coalesce(
               unitarrivedestdatetime,
               unitleftscenedatetime,
               arrivepatientdatetime,
               unitarrivescenedatetime,
               unitenroutedatetime,
               unitdispatchdatetime
           ) as enddt
      from raw.jocomedactincidents m
	  left outer join clean.jocojococlient c
	  on c."source" = 'JOCOMEDACTINCIDENTS.RCDID'
	  and c.hash_sourceid = hash_rcdid;
