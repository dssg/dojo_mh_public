/*
Insert rows from one table into client events.
*/

insert into semantic.client_events (joid, event_date, event_type, table_name)
select 
    joid,
    fix_date({event_date}::varchar) as event_date,
    '{event_type}' as event_type,
    '{table_name}' as table_name
from {schema}.{table_name}
where joid is not null
and {event_date} is not null;
