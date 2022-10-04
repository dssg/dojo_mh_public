coalesce({agg_func}({agg_arg}) filter (
    where t.{knowledge_date} between c.as_of_date - interval '{interval}' and c.as_of_date
    {add_filter}
), {impute_agg}) as {feature_name}