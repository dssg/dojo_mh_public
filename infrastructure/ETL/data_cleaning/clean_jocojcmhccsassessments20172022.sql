/*
 * Create clean table for jocojcmhccsassessments20172022
 * Preliminary selection of most important columns
*/

set role :role;
drop table if exists clean.jocojcmhccsassessments20172022;


create table clean.jocojcmhccsassessments20172022 as (
  select
    client.joid as joid,
    fix_char(patid) as patid,
    time_to_date(columbia_assessment_date) as assessment_date,
    time_to_date(data_entry_date) as data_entry_date,
    fix_varchar(aborted_attempt_dsc) as aborted_attempt_dsc,
    fix_varchar(innterupt_attempt_dsc) as innterupt_attempt_dsc,
    fix_varchar(act_suicid_attmpt_dsc) as act_suicid_attmpt_dsc,
    -- These are not from the Columbia risk assessment tool itself I believe
    fix_yesno_response(aborted_attempt_3m_value) as aborted_attempt_3m,
    fix_yesno_response(aborted_attempt_lt_value) as aborted_attempt_lifetime,
    fix_yesno_response(act_suicid_attmpt_3m_value) as actual_attempt_3m,
    fix_yesno_response(act_suicid_attmpt_lt_value) as actual_attempt_lifetime,
    fix_yesno_response(innterupt_attempt_3m_value) as interupt_attempt_3m,
    fix_yesno_response(innterupt_attempt_lt_value) as interupt_attempt_lifetime,
    fix_yesno_response(nonsuicidal_self_harm_3m_value) as nonsuicidal_self_harm_3m,
    fix_yesno_response(nonsuicidal_self_harm_lt_value) as nonsuicidal_self_harm_lifetime,
    fix_yesno_response(prep_behavior_3m_value) as prep_behavior_3m,
    fix_yesno_response(prep_behavior_lt_value) as prep_behavior_lifetime,
    -- The following are from the Columbia risk assessment tool
    -- See https://cssrs.columbia.edu/the-columbia-scale-c-ssrs/about-the-scale/
    fix_yesno_response(q1_inthepastmonth_value) as q1_pastmonth,
    fix_yesno_response(q1_inyourlifetime_value) as q1_lifetime,
    fix_yesno_response(q2_inthepastmonth_value) as q2_pastmonth,
    fix_yesno_response(q2_inyourlifetime_value) as q2_lifetime,
    fix_yesno_response(q3_inthepastmonth_value) as q3_pastmonth,
    fix_yesno_response(q3_inyourlifetime_value) as q3_lifetime,
    fix_yesno_response(q4_inthepastmonth_value) as q4_pastmonth,
    fix_yesno_response(q4_inyourlifetime_value) as q4_lifetime,
    fix_yesno_response(q5_inthepastmonth_value) as q5_pastmonth,
    fix_yesno_response(q5_inyourlifetime_value) as q5_lifetime,
    thought_frequency_pm_value as suic_thought_freq_pastmonth,
    thought_frequency_lt_value as suic_thought_freq_lifetime,
    thought_duration_pm_value as suic_duration_freq_pastmonth,
    thought_duration_lt_value as suic_duration_freq_lifetime
    from raw.jocojcmhccsassessments20172022 a
    left join clean.jocojococlient client
    on client.sourceid = a.patid
    and client.source = 'JOCOJCMHCDEMOGRAPHICS.PATID'
);
