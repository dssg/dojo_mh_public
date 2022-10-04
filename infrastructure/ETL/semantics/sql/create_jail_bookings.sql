/*
Create empty jail bookings semantic table.
*/

set role :role;

drop table if exists semantic.jail_bookings;

create table semantic.jail_bookings as
select
    joid,
    start_date,
    end_date,
    min(table_name) as table_name --- For DoCo inmates in the Johnson County jail, remove the duplicate row and keep DoCo table name
from (
		select
		    joid,
		    servicebegindate::date as start_date,
		    serviceenddate::date as end_date,
		    'joco110hsccclientservice2' as table_name
		from semi_clean.joco110hsccclientservice2 jh
		where joid is not null
		    and serviceenddate is not null
		    and servicetype in ('Jail Booking', 'BOOK')
		union
		select
		    joid,
		    case
			    when bk_dt_40 = '' then null
			    else bk_dt_40::date
			end as start_date,
			case
			    when rel_date_88 = '' then null
			    else rel_date_88::date
			end as end_date,
		    'jocojimsinmatedata' as table_name
		from semi_clean.jocojimsinmatedata jh
		where joid is not null
		    and rel_date_88 is not null
		   and bk_dt_40 is not null
		union
		select
		    joid,
		    case
			    when bk_dt_40 = '' then null
			    else bk_dt_40::date
			end as start_date,
			case
			    when rel_date_88 = '' then null
			    else rel_date_88::date
			end as end_date,
		    'jocojimsjuvinmatedata' as table_name
		from semi_clean.jocojimsjuvinmatedata jh
		where joid is not null
		    and rel_date_88 is not null
		   and bk_dt_40 is not null
) t
group by t.joid, t.start_date, t.end_date;
