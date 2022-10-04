set role :role;

-- DROP STATEMENTS
drop function if exists fix_char(x char) cascade;
drop function if exists fix_varchar(x char) cascade;
drop function if exists isnumeric(x char) cascade;
drop function if exists fix_char_to_int(x char) cascade;
drop function if exists fix_char_to_float(x char) cascade;
drop function if exists fix_date(x char) cascade;
drop function if exists time_to_date(x timestamp) cascade;
drop function if exists fix_yesno_response(x varchar) cascade;
drop function if exists fix_yes_no(x char) cascade;
drop function if exists fix_yes_no_int(x char) cascade;
drop function if exists fix_sex(x char) cascade;
drop function if exists fix_race(x char) cascade;
drop function if exists fix_zip(x char) cascade;
drop function if exists fix_age(x char) cascade;
drop function if exists fix_time(x char) cascade;
drop function if exists remove_ks(x char) cascade;
drop function if exists fix_city(x char) cascade;
drop function if exists joco_resident(tractid char, address char) cascade;
drop function if exists fix_state(x char) cascade;
drop function if exists categorize_mhc_program(x char) cascade;
drop function if exists categorize_mhc_discharge_reason(x char) cascade;
drop function if exists fix_and_check_date(x char) cascade;
drop function if exists classify_diagnosis(x char) cascade;

-- FUNCTIONS
create function fix_char(x char) returns char AS $$
    select
    case
        -- Many strings have backspace characters (7f in hex), so find and remove them.
        when length(regexp_replace(btrim(x), encode(decode('7f', 'hex'), 'escape'), '')) = 0 then null
        else upper(regexp_replace(btrim(x), encode(decode('7f', 'hex'), 'escape'), ''))
    end as _;
$$ LANGUAGE SQL;

create function fix_varchar(x char) returns varchar AS $$
    select
    case
        -- Many strings have backspace characters (7f in hex), so find and remove them.
        when length(regexp_replace(btrim(x), encode(decode('7f', 'hex'), 'escape'), '')) = 0 then null
        else upper(regexp_replace(btrim(x), encode(decode('7f', 'hex'), 'escape'), ''))
    end as _;
$$ LANGUAGE SQL;

create function isnumeric(x char) returns boolean AS $$
    select
    case
        when fix_char(x) ~ '^[0123456789]+[.]?[0123456789]*$' then TRUE
        else FALSE
    end as _;
$$ LANGUAGE SQL;

create function fix_char_to_int(x char) returns int AS $$
    select
    case
        when char_length(fix_char(x)) = 0 then null
        when isnumeric(fix_char(x)) then x::int
    end as _;
$$ LANGUAGE SQL;

create function fix_char_to_float(x char) returns float AS $$
    select
    case
        when length(fix_char(regexp_replace(x,'[^0-9.]','','g'))) is null then null
        else regexp_replace(x,'[^0-9.]','','g')::float
    end as _;
$$ LANGUAGE SQL;

-- Catch invalid dates such as 02/34/17
create or replace function is_date(s varchar) returns boolean as $$
begin
	  if s is null then
		     return false;
		  end if;
		  perform s::date;
		  return true;
		exception when others then
			  return false;
end;
$$ language plpgsql;

-- Catch strings that are not convertable to dates
create or replace function is_date2(x text) returns date as $$
begin
    begin
        return to_date(x, 'MM/DD/YYYY');
    exception when others then
        begin
            return to_date(x, 'YYYY/MM/DD');
        exception when others then
        return null;
        end;
    end;
end;
$$ language plpgsql;

create function fix_date(x char) returns date AS $$
    select
    case -- could add more checks below
        when x LIKE '%ý%' then null
        when is_date2(x) is null then null
        when x = '' then null
        when char_length(regexp_replace(x, '[^0-9]', '', 'g')) = 6 then
            case
                -- when the year is converted to a year in the future, convert it to the 1900s
                when date_part('year', to_date(x, 'MM/DD/YY')) > date_part('year', CURRENT_DATE)
                    then (to_date(x, 'MM/DD/YY') - interval '100 years')::date
                else to_date(x, 'MM/DD/YY')
            end
        when char_length(regexp_replace(x, '[^0-9]', '', 'g')) = 8 then
              case
				  when date_part('year', x::date) < date_part('year', CURRENT_DATE - interval '100 years') then null
				  else x::date
			  end
        else null
    end as _;
$$ LANGUAGE SQL;

-- Slower but more careful fix_date. Will catch invalid dates.
create function fix_and_check_date(x char) returns date AS $$
    select
    case -- could add more checks below
        when x LIKE '%ý%' then null
		when char_length(regexp_replace(x , '[^0-9]', '', 'g')) not in (6, 8) then null
		when not is_date(x) then null
        else to_date(regexp_replace(x, '[^0-9]', '', 'g'), 'MMDDYYYY')
    end as _;
$$ LANGUAGE SQL;

create function time_to_date(x timestamp) returns date as $$
    select
    case -- could add more checks below
        when char_length(regexp_replace(split_part(x::varchar,' ',1), '[^0-9]', '', 'g')) = 6
            then to_date(split_part(x::varchar,' ',1), 'YY-MM-DD')
        when char_length(regexp_replace(split_part(x::varchar,' ',1), '[^0-9]', '', 'g')) = 8
           then to_date(split_part(x::varchar,' ',1), 'YYYY-MM-DD')
        else null
    end as _;
$$ LANGUAGE SQL;

create function fix_yesno_response(x varchar) returns boolean AS $$
    select
    case
        when fix_varchar(x) = 'Y' or fix_varchar(x) = 'YES' then TRUE
        when fix_varchar(x) = 'N' or fix_varchar(x) = 'NO' then FALSE
        else NULL
    end as _;
$$ LANGUAGE SQL;

create function fix_yes_no(x char) returns boolean AS $$
    select
    case -- i think we want more checks/constraints/errors in here
        when length(fix_char(x)) = 0 then null
        when fix_char(x) = 'N' then false
        when fix_char(x) = 'Y' then true
        else null
    end as _;
$$ LANGUAGE SQL;

create function fix_yes_no_int(x char) returns int AS $$
    select
    case -- i think we want more checks/constraints/errors in here
        when length(fix_char(x)) is null then null
        when fix_char(x) = 'N' then 0
        when fix_char(x) = 'Y' then 1
        else null
    end as _;
$$ LANGUAGE SQL;

create function fix_sex(x char) returns char AS $$
    select
    case -- i think we want more checks/constraints/errors in here
        when length(fix_char(x)) = 0 then null
        when fix_char(x) like 'M%' then 'MALE'
        when fix_char(x) like 'F%' then 'FEMALE'
        else null
    end as _;
$$ LANGUAGE SQL;

create function fix_race(x char) returns char AS $$
    select
    case -- i think we want more checks/constraints/errors in here
        when length(fix_char(x)) = 0 then null
        when fix_char(x) similar to '(AM)%' or fix_char(x) = 'I' then 'I'
        when fix_char(x) similar to '(AS)%' or fix_char(x) = 'A' then 'A'
        when fix_char(x) similar to '(B)%' then 'B'
        when fix_char(x) similar to '(C)%' or fix_char(x) similar to '(W)%' then 'W'
        when fix_char(x) similar to '(HI)%' then 'H'
        when fix_char(x) similar to '(O)%' then 'O'
        when fix_char(x) similar to '(NAT)%' or fix_char(x) similar to '(HA)%' or fix_char(x) = 'P' then 'P'
        when fix_char(x) similar to '(U)%' or fix_char(x) = '?' then null
        else null
    end as _;
$$ LANGUAGE SQL;

create function fix_zip(x char) returns char AS $$
    select
    case
        when x = '00000' then null
        when x = '99999' then null
        else x
    end as _;
$$ LANGUAGE SQL;

create function fix_age(x char) returns integer AS $$
    select
    case
        when isnumeric(x) and x::int<150 then fix_char(x)::integer
        else null
    end as _;
$$ LANGUAGE SQL;

create or replace function fix_real(x char) returns real as $$
begin
    if char_length(x) = 0 then
        return null;
    end if;
    return x::real;
end;
$$ language plpgsql;

create function fix_time(x char) returns time AS $$
    select
    case -- not sure on this
        when char_length(substring(regexp_replace(x, '[^0-9]', '', 'g') from '.{0,6}$')) = 4
            or char_length(substring(regexp_replace(x, '[^0-9]', '', 'g') from '.{0,6}$')) = 6
            then to_timestamp(regexp_replace(x, '[^0-9]+', ''), 'HH24MI')::TIME
        else null
    end as _;
$$ LANGUAGE SQL;

create function remove_ks(x char) returns char AS $$
    -- Remove the ending " KS", " KS.", ', KS", or ", KS." from a city
    select
    regexp_replace(x, ',? +KS\.?$', '') as _;
$$ LANGUAGE SQL;

create function fix_city(x char) returns char as $$
    -- For communities in Johnson County (and Kansas City), normalize the names
    select
    case
        when levenshtein_less_equal(x, 'OLATHE', 2) <= 2
            and x not in ('PLATTE', 'BLYTHE') -- Only a few chars different but actually different cities!
            or x in ('66061', '66062', 'OL') -- Other versions of Olathe
            or x ~ 'OLATHE' -- For cities like "OLATHE KANSAS'
            then 'OLATHE'
        when levenshtein_less_equal(x, 'OVERLAND PARK', 4) <= 4
            and x !~ '^[RP]O[EW]{0,2}L(A|OA|AL)(N|M)D?' -- Don't get Roeland Park (or its misspellings)
            and x !~ '(SUN ?LAND|OAKLAND|WOODLAND|ROLEAND|ORLAND PARK)' -- Don't got other X Park cities
            or x in ('OVERLAND', 'OV. PARK', 'OVER', 'OVERPARK', 'OPKS', 'OVLND PK') -- Shorthand spellings
            or x ~ '^(O|0)\.? ?P;?\.?$' -- "O.P." and the like
            or x ~ '^OV\.? ?P(AR)?K\.?$' -- "OV. PARK" and the like
            then 'OVERLAND PARK'
        when levenshtein_less_equal(x, 'SHAWNEE', 3) <= 3
            and x not in ( -- Only a few chars different but actually different cities!
                'SANTEE',
                'CHANGE',
                'OZAWKEE',
                'SHAWNEE MS',
                'PAWNEE',
                'HAWLEY',
                'SHAKOPEE',
                'SHINER'
            )
            or x ~ '(66203|66217)' -- Shawnee zip codes
            then 'SHAWNEE'
        when levenshtein_less_equal(x, 'LENEXA', 2) <= 2
            and x !~ '(SENECA|GENEVA)' -- Only a few chars different but actually different cities!
            or x in ('LENEXALENEXA', 'LENEXA KANASAS', 'LENEXA DR') -- Different versions of Lenexa
            or x ~ '66216' -- Lenexa zip code
            then 'LENEXA'
        when levenshtein_less_equal(x, 'KANSAS CITY', 3) <= 3
            and x != 'ARKANSAS CITY'
            then 'KANSAS CITY'
        when levenshtein_less_equal(x, 'GARDNER', 1) <= 1
            or x in ('66030', 'GARNDER', 'GARDNER LAKE') -- Other versions of Gardner
            then 'GARDNER'
        when levenshtein_less_equal(x, 'BONNER SPRINGS', 2) <= 2
            or x in ('BONNER', 'BONNOR SPIRNGS') -- Different versions of Bonner Springs
            or x ~ 'BONNER SPGS' -- Catches things like 'BONNER SPGS KANSAS'
            then 'BONNER SPRINGS'
        when levenshtein_less_equal(x, 'DE SOTO', 3) <= 3
            and x != 'DENSON' -- Only a few chars different ut actually a different city!
            or x = '66018' -- Zip code for De Soto
            then 'DE SOTO'
        when levenshtein_less_equal(x, 'EDGERTON', 1) <= 1
            or x = 'EDGERTON RD'
            then 'EDGERTON'
        when levenshtein_less_equal(x, 'LEAWOOD', 1) <= 1
            or x in ('LEAWODO', 'LEE WOOD')
            then 'LEAWOOD'
        when levenshtein_less_equal(x, 'MERRIAM', 2) <= 2
            or x = 'MERRIAM WOODS'
            then 'MERRIAM'
        when levenshtein_less_equal(x, 'MISSION', 2) <= 2
            or x ~ '^MISSION\.*$' -- Finds things like "MISSION....."
            then 'MISSION'
        when levenshtein_less_equal(x, 'MISSION HILLS', 3) <= 3 then 'MISSION HILLS'
        when levenshtein_less_equal(x, 'MISSION WOODS', 3) <= 3 then 'MISSION WOODS'
        when levenshtein_less_equal(x, 'PRAIRIE VILLAGE', 5) <= 5
            or x in ('PRAIRIE', 'PR.VILLAGE', 'PRAIRE', 'PRARIE VI')
            or x ~ '^P\.?V\.?$' -- Abbreviations like "P.V."
            and x != 'PRAIRIEVILLE'
            then 'PRAIRIE VILLAGE'
        when levenshtein_less_equal(x, 'ROELAND PARK', 2) <= 2
            and x not in ('OERLAND PARK', 'ORLAND PARK')
            or x in ('ROELAND', 'ROEPARK')
            then 'ROELAND PARK'
        when levenshtein_less_equal(x, 'SHAWNEE MISSION', 7) <= 7
            or x = 'SM'
            then 'SHAWNEE MISSION'
        when levenshtein_less_equal(x, 'SPRING HILL', 2) <= 2
            or x ~ '^SPRING HILL \(' -- Catching things like "SPRING HILL (TOWNSHIP)"
            then 'SPRING HILL'
        when levenshtein_less_equal(x, 'CLEARVIEW CITY', 4) <= 4
            or levenshtein_less_equal(x, 'CLEARVIEW', 2) <= 2 -- Sometimes "CITY" is omitted
            then 'CLEARVIEW CITY'
        when levenshtein_less_equal(x, 'STILWELL', 2) <= 2 then 'STILWELL'
        when x ~ 'FAIRWAY'
            or x in ('FARIWAY', 'FAIRYWAY')
            or x = 'MISSION HIGHLANDS' -- Found one odd example of this with Fairway zip code
            then 'FAIRWAY '
        when levenshtein_less_equal(x, 'LAKE QUIVIRA', 5) <= 5
            and x !~ '(BUTLER|ZURICK)'
            or x ~ '^LAKE QUIVIRA'
            then 'LAKE QUIVIRA'
        when levenshtein_less_equal(x, 'WESTWOOD', 1) <= 1 then 'WESTWOOD'
        when levenshtein_less_equal(x, 'WESTWOOD HILLS', 1) <= 1 then 'WESTWOOD HILLS'
        when levenshtein_less_equal(x, 'STANLEY', 2) <= 2
            and x != 'STAPLES'
            then 'STANLEY'
        when x in ('OXFORD', 'OXFORD KANSAS') then 'OXFORD'
        when levenshtein_less_equal(x, 'EUDORA', 1) <= 1
            and x != 'ELDORA'
            or x = 'EUODRA'
            then 'EUDORA'
        when levenshtein_less_equal(x, 'BUCYRUS', 3) <= 3
            and x != ('BURNS')
            then 'BUCYRUS'
        when levenshtein_less_equal(x, 'NEW CENTURY', 5) <= 5
            and x != 'OAK CENTER'
            or x ~ '^NEW CENTURY' -- Cities like 'NEW CENTURY AIRPORT'
            then 'NEW CENTURY'
        when x in ('', '`', '.') then null
        else x
    end as _;
$$ LANGUAGE SQL;

create function joco_resident(tractid char, address char) returns bool as $$
    select
    case
        -- JoCo geocoding only returns tractids that are in JoCo and
        -- otherwise returns NULL, but just in case that changes, confirm that
        -- the tractid is in JoCo and return true
        when left(tractid, 5) = '20091' then true
        -- If the tractid isn't in JoCo, verify whether the address could not
        -- have been geocoded. If there is no street address, then it could not
        -- have been geocoded and we want to mark it as NULL
        -- Note: Our previous attempt to use city, state, and zip information
        -- to identify Johnson County residents ended up being far too
        -- inclusive to be useful, so we will not fall back on that method.
        when address is null then null
        -- Otherwise, we will assume that the address could have been geocoded
        -- and that it was not in Johnson County
        else false
    end as _;
$$ LANGUAGE SQL;

create function fix_state(x char) returns char as $$
  select
  case
      when x in ('K', 'KA') then 'KS'
      when x = ' ' then null
      else x
  end as state;
$$ LANGUAGE SQL;

create function categorize_mhc_program(x char) returns char AS $$
    select
        case
            when x like '%ACT%' then 'ACT'
            when x like '%ADOLESCENT%'
                or x like '%YOUTH%'
                or x like '%PEDIATRICS%' then 'YOUTH TREATMENT'
            when x like '%MH%'
                or x like '%MENTAL HEALTH%' then 'MENTAL HEALTH'
            when x like '%FAMILY%%'
                or x like '%FAM FOCUS%' then 'FAMILY'
            when x like '%GAMBLING%' then 'GAMBLING'
            when x like '%EMPLOYMENT%'
                or x like '%VOCATIONAL%' then 'EMPLOYMENT'
            when x like '%ALCOHOL%' or x like '%DRUG%'
                or x like '%SUBSTANCE ABUSE%'
                or x like '%SUD%'
                or x like '%DETOXIFICATION%' then 'SUBSTANCE ABUSE'
            when x ~ '^PRE' and x like '%ADMIT%' then 'PRE-ADMIT'
            when x like '%CSS%' then 'CSS'
            when x like '%CDS%' then 'CDS'
            else x
        end as _;
$$ LANGUAGE SQL;

create function categorize_mhc_discharge_reason(x char) returns char as $$
  select
        case
            when x ~ '^DEATH' then 'DEATH'
            when x ~ '^FURTHER CARE NOT AVAILABLE'
                then 'FURTHER CARE NOT AVAILABLE'
            when x ~ '^MUTUAL DECISION'
                then 'MUTUAL DECISION'
            when x like '%NOT COMPLETE%'
                and (x like '%AGCY DECISION%'
                or x like '%AGENCY DECISION%')
                then 'TREATMENT NOT COMPLETE: AGENCY DECISION'
            when (x like '%NOT COMPLETE%' and x like '%CLIENT DECISION%')
                or x like '%DROPPED OUT%' then 'TREATMENT NOT COMPLETE: CLIENT DECISION'
            when x like '%TREATMENT COMPLETE%'
                or x like '%EVALUATION COMPLETE%' then 'COMPLETED'
            when x like '%PRE%' and x like '%ADMIT%' and x like '%ADMITTED%'
                then 'PRE-ADMIT: CLIENT ADMITTED'
            when x like '%CLIENT MOVED%'
                then 'CLIENT MOVED'
            when x ~ '^TRANSFER'
                then 'TRANSFER'
            when x like '%TREATMENT REJECTED%'
                or x like '%REFUSED%'
                then 'TREATMENT REJECTED'
            when x ~ 'CANCELLED|CANCELED'
                or x like '%NO SHOW%'
                then 'TREATMENT CANCELLED'
            else x
  end as _;
$$ LANGUAGE SQL;

create function classify_diagnosis(x char) returns char as $$
-- Classify diagnosis into categories (nees to be upper case)
  select
    CASE
        WHEN x ~ '(INTELLECTUAL DIS|AUTIS|ASPERGER)'
            OR x ~ 'ATT(ENTIO)?N( |-)DEFI?CI?T'
            OR x ~ '(DEVELOPM|LANGUAGE|TOURETTE)'
            OR x ~ '( TIC |RETARD|FETAL ALC)'
            OR x ~ '^(TIC)'
            THEN 'NEURODEVELOPMENTAL'
        WHEN (
                x ~ '(SCHIZ|DELUSION|PSYCHOTIC|CATATON)'
                OR x ~ 'PSYCHOS(I|E)S'
            )
            AND (
                x !~ 'DEMENTIA'
                AND x !~ '^(BIPOLAR|MAJOR DEP|CANNABIS)'
                AND x !~ '^(MANIC|OPIOID|OTH STIM)'
                AND x !~ '^OTHER (PSYCHOACTI|STIMULA)'
                AND x !~ 'NONPSYCHOTIC'
            )
            THEN 'PSYCHOTIC'
        WHEN x ~ '(BIPOLAR|CYCLOTHYM|MANIC D|MANIC E)'
            THEN 'BIPOLAR'
        WHEN x ~ '(DISRUPTIVE MOOD|DEPRESS|DYSTHYM)'
            AND x !~ '^ADJUSTMENT'
            AND x !~ 'DEMENTIA'
            OR x ~ 'PREMENSTRUAL'
            THEN 'DEPRESSIVE'
        WHEN x ~ '(ANXIETY|PHOBIA|PANIC)'
            AND x !~ '^(ADJUST|ALCOH|CANNAB|HALLUC)'
            THEN 'ANXIETY'
        WHEN x ~ '(OBSESSIVE|DYSMORPHIC|HOARDING)'
            OR x ~ '(TRICHOTILLO|EXCORIATION)'
            THEN 'OBSESSIVE-COMPULSIVE'
        WHEN (
                x ~ '(REACTIVE ATTACH|DISINHIB|STRESS)'
                OR x ~ '(ADJUSTMENT|TRAUMA)'
            )
            AND (
                x !~ '(SHOULDER|AMPUTATIO|BIRTH|BRAIN)'
                OR x !~ '(SUBDURAL)'
            )
            THEN 'TRAUMA- OR STRESSOR- RELATED'
        WHEN x ~ '(DISS?OCIA|DEPERSONAL|DEREALIZ)'
            OR x ~ 'MULTIPLE PERSONALITY'
            THEN 'DISSOCIATIVE'
        WHEN x ~ '(SOMATIC|ILLNESS A|CONVERSION)'
            OR x ~ '(FACTITIOUS|SOMATOFORM)'
            THEN 'SOMATIC SYMPTOM'
        WHEN (
                x ~ '(PICA|RUMINATION|FOOD|ANOREXIA)'
                OR x ~ '(BULIMIA|BINGE|FEEDING|EATING)'
            )
            AND x !~ 'ALLERGY'
            THEN 'FEEDING OR EATING'
        WHEN x ~ '(ENURESIS|ENCOPRESIS|ELIMINATION)'
            THEN 'ELIMINATION'
        WHEN x ~ '(SOMN|NARCOL|SLEEP|NIGHTM|RESTLESS)'
            THEN 'SLEEP-WAKE'
        WHEN x ~ '(GENDER|PSYCHOSEXUAL IDENTITY)'
            OR x ~ 'SEX REASSIGN'
            THEN 'GENDER DYSPHORIA'
        WHEN x ~ '(OPPOSITION|EXPLO|CONDUCT|ANTISOC)'
            OR x ~ '(PYRO|KLEPTO|IMPULSE)'
            THEN 'DISRUPTIVE, IMPULSE-CONTROL, AND CODUCT'
        WHEN (
                x ~ '(NEUROCOG|DELIRIUM|ALZH|DEMENTI)'
                OR x ~ 'COGNITIVE DECLINE'
            )
            AND x !~ '^(ALCOHOL|OPIOID|OTHER STIMULA)'
            THEN 'NEUROCOGNITIVE'
        WHEN x ~ 'PERSONALITY'
            OR x = 'PARANOIA'
            THEN 'PERSONALITY'
        WHEN x ~ '(VOYUER|EXHIBI|FROTT|MASOCH|SADISM)'
            OR x ~ '(PEDOPH|FETISH|TRANSV|PARAPHILIC)'
            THEN 'PARAPHILIC'
        WHEN (
                x ~ '(SUBSTANCE|ALCOHOL)'
                OR x ~ '(CANNAB|HALLUC|PHENCY|INHAL)'
                OR x ~ '(OPIOID|SEDATIVE|STIMULANT)'
                OR x ~ '(COCAINE|DRUG)'
                OR x ~ '(NICOTINE)'
                OR x ~ '(AMPHETAMI)'
            )
            AND (
                x ~ '(RELATE|USE|INDUCE|INTOX|WITHD)'
                OR x ~ '(DEPENDENCE|PERCEPT|DEP\.)'
            )
            OR x ~ '^AMPHET'
            THEN 'SUBSTANCE-RELATED OR ADDICTIVE'
        WHEN (
                x ~ '(CAFFEINE)'
                OR x ~ '(TOBACCO)'
            )
            AND (
                x ~ '(RELATE|USE|INDUCE|INTOX|WITHD)'
                OR x ~ '(DEPENDENCE|PERCEPT|DEP\.)'
            )
            OR x ~ 'GAMBLING'
            THEN 'OTHER SUBSTANCE-RELATED OR ADDICTIVE'
        WHEN
            x ~ '(SELF-HARM|SUIC)'
            THEN 'SUICIDE IDEATION OR SELF-HARM'
        WHEN
            x ~ '(PHYSICAL ABUSE|SEXUAL ABUSE|PSYCHOLOGICAL ABUSE|EMOTIONAL ABUSE|CHILD ABUSE|PARTNER ABUSE)'
            OR x ~ '(NEGLECT|MALTREATMENT|ABANDONMENT)'
            OR x ~ '(ABUSE IN CHILDHOOD|ABUSE OF ADULT)'
            THEN 'ABUSE OR NEGLECT'
        WHEN
            x ~ '(DISAPPEARANCE OF FAMILY MEMBER|DEATH OF FAMILY MEMBER)'
            OR x ~ '(CHILDHOOD EMOTIONAL DISORDER|CHILD CONFLICT)'
            OR x ~ '(PARENT-CHILD|PARENT-FOSTER|PARENT-BIOLOGICAL CHILD CONFLICT|SIBLING RIVALRY|SIBLING RELATIONAL PROBLEM)'
            OR x ~ '(PARTNER RELATIONAL PROBLEM|RELATIONSHIP WITH SPOUSE OR PARTNER|RELATIONAL PROBLEM)'
            THEN 'OTHER FAMILY'
        WHEN
            x ~ '(HOMELESS|HOUSING|RESIDENTIAL INST|LODGER|LANDLORD|LIVING ALONE)'
            OR x ~ '(LOW INCOME|OCCUPATION|ECONOMIC|UNEMPLOY)'
            OR x ~ '(SOCIAL INS|WELFARE SUP)'
            THEN 'HOUSING OR ECONOMIC'
        WHEN
            x ~ '(PRISON|CRIMINAL)'
            THEN 'CRIMINAL'
        WHEN
            x ~ '(ASSAULT)'
            THEN 'ASSAULT'
        WHEN
            x ~ '(SOCIAL EXCL|REJECTION|SUPPORT GROUP|SOCIAL ENV)'
            OR x ~ 'RELIGIOUS OR SPIRITUAL PROBLEM'
            OR x ~ 'SELF-ESTEEM'
            THEN 'OTHER SOCIAL'
        WHEN
            x ~ '(BEHAVIOURAL DISORDER|MENTAL DISORDER|MOOD DISORDER|EMOTIONAL DISORDER)'
            THEN 'OTHER MENTAL DISORDER'
        ELSE 'OTHER'
    END AS diagnosis_classification;
$$ LANGUAGE SQL;

-- EXAMPLES
/*select * -- hacky to get full table
from (select
    prob_no_0,
    fix_char_to_int(prob_no_0) as prob_no,
    court_case_1,
    court_case_1 as court_case,
    mni_2,
    fix_char_to_int(mni_2) as mni_no,
    city_4,
    city_4 as city,
    st_5,
    fix_char(st_5) as state,
    zip_6,
    fix_char_to_int(zip_6) as zip, -- weird & why skip numbers?
    prob_due_dt_16,
    fix_date(prob_due_dt_16) as prob_due_dt,
    actual_pb_compl_dt_17,
    fix_date(actual_pb_compl_dt_17) as actual_pb_compl_dt,
    psp_hours_18,
    fix_char_to_float(psp_hours_18) as psp_hours,
    psp_comp_19,
    fix_yes_no(psp_comp_19) as psp_comp
    from (select distinct * from raw.jocojimsprobationdata) as probation) as probation
where probation is not null;*/
