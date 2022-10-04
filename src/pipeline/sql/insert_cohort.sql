/*
Insert cohort rows.
Comments: 
	- Currently assigning county based on last interaction with system
	- We remove any person who died before the as_of_date
*/

insert into modeling.cohort
with ce_partitioned as (
	select 
		joid as joid,
    	'{as_of_date}'::date as as_of_date,
    	table_name,
		row_number() OVER (PARTITION BY joid ORDER BY event_date DESC) as date_ranked
	from semantic.client_events
	where joid is not null
	    and event_date >= date '{as_of_date}' - interval '{interval_back}'
	    and event_date <=  date '{as_of_date}'
		and table_name not in {tables_excluded} 
)
select distinct
	joid,
	as_of_date,
	case
		when table_name in ({doco_tables}) then 'doco'
		when table_name in ({joco_tables}) then 'joco'
		else null
	end as county
from ce_partitioned
where date_ranked = 1
	and joid not in (
		select joid
			from clean.jocodcmexoverdosessuicides
			where dateofdeath <= '{as_of_date}'::date 
				and joid is not null
	) and joid not in (
		select joid
		from clean.jocojcmexoverdosessuicides
		where dateofdeath <= '{as_of_date}'::date	
			and joid is not null
);