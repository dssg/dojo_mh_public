set role {psql_role};
--drop table if exists features.{feature_table_name};

create table features.{feature_table_name} as
with cohort as (
    select joid, as_of_date
    from modeling.cohort
    where as_of_date in ({as_of_dates})
)
select c.joid, c.as_of_date, {feature_cols}
from cohort c
left join {from_arg} t
on c.joid = t.joid and t.{knowledge_date} < c.as_of_date
group by 1, 2;