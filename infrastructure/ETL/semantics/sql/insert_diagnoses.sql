/*
Insert rows to diagnoses semantics table.
*/

insert into semantic.diagnoses (joid, diag_date, prim_diag, sec_diag, prim_diag_icd10, 
	sec_diag_icd10, mh_diag_flag, prim_diag_class, sec_diag_class, table_name)
select 
	joid,
	diag_date,
	prim_diag,
	sec_diag,
	prim_diag_icd10,
	sec_diag_icd10,
	case
		when prim_diag_icd10 in (select icd10_code from clean.mh_icd10_codes) then true
		when sec_diag_icd10 in (select icd10_code from clean.mh_icd10_codes)  then true
		else false
	end as mh_diag_flag,
	prim_diag_class,
	sec_diag_class,
	table_name
from (select
		joid,
		admission_date as diag_date,
		primarydiagnosis as prim_diag,
		secondarydiagnosis as sec_diag,
		substr(primarydiagnosis_icd10, 2, length(primarydiagnosis_icd10) - 2) as prim_diag_icd10,
		substr(secondarydiagnosis_icd10, 2, length(secondarydiagnosis_icd10) - 2) as sec_diag_icd10,
		primary_diagnosis_classification as prim_diag_class,
		secondary_diagnosis_classification as sec_diag_class,
		'joco110hsccclientmisc2eadiagnosis' as table_name
	from clean.joco110hsccclientmisc2eadiagnosis jhe
	where joid is not null
	union
	select 
		joid,
		dx_date as diag_date,
		diagnosis_description as prim_diag,
		null as sec_diag,
		dx_code as prim_diag_icd10,
		null as sec_diag_icd10,
		diagnosis_classification as prim_diag_class,
		null as sec_diag_class,
		'jocojcmhcdiagnoses' as table_name
	from clean.jocojcmhcdiagnoses jhe
	where joid is not null
) t;
