/* Create clean table for jocojococlient.

This table makes the following changes:
- Selects only relevant columns
- Converts all string columns to upper
- Converts all missing string values to NULL rather than empty strings or special null values
- Encodes dates

Issues:
- dob currently has incorrect values (dates in the future/very far back)
*/

SET ROLE :role;

DROP TABLE IF EXISTS clean.jocojococlient;

CREATE TABLE clean.jocojococlient
    AS (
    SELECT
        urno,
        id,
        joid,
        fix_char(source) AS source,
        fix_char(sourceid) AS sourceid,
        hash_sourceid,
        fix_char(sourceidpointer) AS sourceidpointer,
        case when dob = '1900-01-01 00:00:00.000' then null else time_to_date(dob) end as dob,
        time_to_date(matchdatetime) AS matchdate,
        fix_char(joidassignedby) AS joidassignedby,
        lastnamepop,
        firstnamepop,
        ssnpop,
        dobpop
    FROM raw.jocojococlient
    WHERE time_to_date(matchdatetime) = (SELECT MAX(time_to_date(matchdatetime)) FROM raw.jocojococlient)
);