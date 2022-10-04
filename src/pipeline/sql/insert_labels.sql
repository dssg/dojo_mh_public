/*
Query to build the labels module.
*/

set role :role;

insert into modeling.{label_tablename} (joid, as_of_date, county, label_name, label)
{cte_query}
select distinct
        co.joid as joid,
        '{as_of_date}'::date as as_of_date,
        co.county as county,
        '{label_name}' as label_name,
        case
                when l.label is null then false else l.label
        end as label
from 	(select *
	from modeling.cohort
	where as_of_date = '{as_of_date}'::date
        and county in ({county})
        ) co
left join (
        {combination_query}
        ) l
        on co.joid = l.joid;
