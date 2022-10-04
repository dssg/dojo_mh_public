/*
Insert rows into the semantic table of demographics. 
*/

insert into semantic.demographics (joid, event_date, table_name, demographics_type, demographics_value)
with demographics_all as (
SELECT distinct joid,
    {event_date} as event_date,
    '{table_name}' as table_name,
    unnest(array[{type_demographics}]) AS demographics_type,
    unnest(array[{type_demographics_raw}]) AS demographics_value
    from {schema}.{table_name}
where joid is not null 
)
select * from demographics_all where demographics_value is not null;
