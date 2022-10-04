/*

Create clean table for jocojcmhcdiagnoses.

This table makes the following changes:
- Add joid from jocojococlient table
- Convert patid to string for matching
- Convert all string columns to upper
- Convert all missing string values to NULL rather than empty strings or special null values
- Encode dates
- Create diagnosis classification column from diagnosis description
- Create self harm and substance use flag
 */

SET ROLE :role;

DROP TABLE IF EXISTS clean.jocojcmhcdiagnoses;

CREATE TABLE clean.jocojcmhcdiagnoses (
	joid int,
	patid varchar,
	dx_date date,
	dx_code varchar,
	diagnosis_description varchar,
	diagnosis_classification varchar,
	substance_use_flag boolean,
	self_harm_flag boolean,
	source_system varchar
);

INSERT INTO clean.jocojcmhcdiagnoses
WITH mhc_diagnoses AS (
			SELECT
				c.joid as joid,
				d.patid::varchar AS patid,
				date(d.dx_date) AS dx_date,
				fix_char(d.dx_code) AS dx_code,
				fix_char(d.diagnosis_description) AS diagnosis_description,
				classify_diagnosis(fix_char(d.diagnosis_description)) AS diagnosis_classification,
				fix_char(d.sourcesystem) AS source_system
				FROM raw.jocojcmhcdiagnoses d
				LEFT OUTER JOIN clean.jocojococlient c
					ON c."source" = 'JOCOJCMHCDEMOGRAPHICS.PATID'
					AND c.sourceid = d.patid::varchar
)
SELECT
	joid,
	patid,
	dx_date,
	dx_code,
	diagnosis_description,
	diagnosis_classification,
	CASE
		WHEN diagnosis_classification = 'SUBSTANCE-RELATED' THEN true
		ELSE false
	END AS substance_use_flag,
	CASE
		WHEN diagnosis_classification = 'SUICIDE IDEATION OR SELF-HARM' THEN true
			ELSE false
		END AS self_harm_flag,
		source_system
	FROM mhc_diagnoses;
