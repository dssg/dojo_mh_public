/*

Create clean jocojcmhcdemographics table.

* Part of this code has been re-used from johnson-county-ddj 2018 project.

Changes:
- Add joid from jocojococlient table
- Remove hash columns
- Convert patid to string for matching
- Convert all string columns to upper
- Convert all missing string values to NULL rather than empty strings or special null values
- Encode dates
- Standardizes race, sex, and location variables (refer to infraestructure/database-setup/ETL/data_cleaning/_functions.sql)
*/

set role :role;

drop table if exists clean.jocojcmhcdemographics cascade;

create table clean.jocojcmhcdemographics as (
    select
        c.joid as joid,
        patid::varchar as patid,
        date(birth_date) as dob,
        fix_race(d.race) as race,
        fix_sex(d.sex) as sex,
        fix_city(remove_ks(fix_char(d.city))) as city,
        fix_state(fix_char(state)) as state,
        fix_zip(left(d.zip_4,5)) as zip,
        fix_zip(d.zip_4) as zip_4,
        joco_resident(tract2010id, encode(d.hash_address, 'hex')) as joco_resident,
        d.tract2010id,
        d.blockgroup2010id,
        d.block2010id,
        fix_char(d.referral_source) as referral_source,
        d.sourcesystem as source_system
    from raw.jocojcmhcdemographics d
    left outer join clean.jocojococlient c
        on c."source" = 'JOCOJCMHCDEMOGRAPHICS.PATID'
		and c.sourceid = d.patid::varchar
);


/* This query creates a convenient version of the JCMHC demographics table to
 * be used for records creation. The raw demographics table contains more than
 * one record per patient for many patients, but we do not have any date
 * information, so we cannot link to events on the basis of when the data were
 * collected. To resolve this issue, this table:
 *     - drops the referral source column, which is the only column that
 *       differs among duplicate entries in AVATAR and is not used in any
 *       records table due to its lack of date
 *     - counts the patient as a Johnson County resident if they were a
 *       resident for *any* demographics entry
 *     - for all other columns, uses any information that is consistent across
 *       *all* demographics entries; otherwise, defaults to NULL
 * This is done per patient per sourcesystem. Since events can be joined to
 * demographics on both of these columns, we will prefer the demographic data
 * associated with the sourcesystem that recorded the event.
 */

DROP TABLE IF EXISTS clean.jocojcmhcdemographics_dedupe;

CREATE TABLE clean.jocojcmhcdemographics_dedupe AS (
    WITH
    combine_records AS (
        SELECT joid,
	       array_agg(DISTINCT patid)        AS patid,
               array_agg(DISTINCT dob)          AS dob,
               array_agg(DISTINCT race)         AS race,
               array_agg(DISTINCT sex)          AS sex,
               max(joco_resident::int)          AS joco_resident,
               array_agg(DISTINCT city)         AS city,
               array_agg(DISTINCT state)        AS state,
               array_agg(DISTINCT zip)          AS zip
          FROM clean.jocojcmhcdemographics
         GROUP BY joid
    )
    SELECT joid,
           unnest(CASE WHEN array_length(patid, 1) = 1 THEN patid else array[null::char] end) AS patid,
           unnest(CASE WHEN array_length(dob, 1) = 1 THEN dob else array[null::date] end) AS dob,
           unnest(CASE WHEN array_length(race, 1) = 1 THEN race else array[null::char] end) AS race,
           unnest(CASE WHEN array_length(sex, 1) = 1 THEN sex else array[null::char] end) AS sex,
           joco_resident,
           unnest(CASE WHEN array_length(city, 1) = 1 THEN city else array[null::char] end) AS city,
           unnest(CASE WHEN array_length(state, 1) = 1 THEN state else array[null::char] end) AS state,
           unnest(CASE WHEN array_length(zip, 1) = 1 THEN zip else array[null::char] end) AS zip
      FROM combine_records
);
