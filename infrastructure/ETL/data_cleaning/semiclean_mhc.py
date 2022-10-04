"""This script is used to populate the semi_clean schema with mhc tables.
Run me to add / update the semi-clean tables. Alternatively, call
clean_mhc_tables().
"""

import re
import pandas as pd
from utils.helpers import get_all_table_names, get_column_names, get_database_connection

schema = 'semi_clean'
# List denoting priority of date field used to populate event_date 
date_order = [
    'columbia_assessment_date', 'assesment_date', 'call_date',
    'data_entry_date', 'effect_date', 'dschg_date', 'svc_date', 'status_date'
]


def clean_cols_list(cols_list, check_len=True):
    if check_len:
        assert len(cols_list) < 2  # Can only have 1 valid col per list (e.g., race or sex)
    cols_list = cols_list[0] if cols_list else None
    return cols_list


def resolve_date_query(date_cols: list[str], col_names: list[str]) -> str:
    """Set up the query for dates. We must resolve which field to use
    to populate event_date varies. This function uses the first valid
    field in date_order. 
    """
    if date_cols == []:
        raise Exception(f'no matching date col in {col_names}.')
    
    date_cols = set(date_cols) & set(col_names) & set(date_order)
    date_fields = []
    for field in date_order:
        if field in date_cols:
            date_fields.append(field)
    #q = f"fix_and_check_date(coalesce({', '.join(date_fields)})::char) as event_date"
    q = f"{', '.join(date_fields)}::date as event_date"
    return q


def clean_mhc_tables():
    """Driver function."""
    db_conn = get_database_connection()
    tnames = get_all_table_names(db_conn, schema='raw')
    mhc_tables_re = re.compile('mhc')

    # Ignore tables already cleaned or not relevant
    clean_tables = set(get_all_table_names(db_conn, schema='clean'))  # already cleaned tables
    ignore_tables = clean_tables | set(['jocojcmhccalldetails'])  # tables to not bring into the semi-clean schema

    # Keep only mhc tables that we do not wish to ignore
    tnames = [tname for tname in tnames if mhc_tables_re.search(tname) and tname not in ignore_tables]
    print(f'Tables to add / update: {tnames}')

    # compile regex patterns 
    race_re = re.compile('race')
    sex_re = re.compile('sex(?!ual|off)')
    date_re = re.compile('date')
    dob_re = re.compile('dob')
    for tname in tnames:
        print(f'table_name: {tname}')
        col_names = get_column_names(db_conn, tname)
        
        # Get the race column
        race_col = [s for s in col_names if race_re.search(s) is not None]
        race_col = clean_cols_list(race_col)
        race_q = f'fix_race({race_col}) as race' if race_col else None

        # Get the sex column
        sex_col = [s for s in col_names if sex_re.search(s) is not None]
        sex_col = clean_cols_list(sex_col)
        sex_q = f'fix_sex({sex_col}) as sex' if sex_col else None

        # Get the dob column
        dob_col = [s for s in col_names if dob_re.search(s) is not None]
        dob_col = clean_cols_list(dob_col)
        dob_q = f'fix_dob({dob_col}) as dob' if dob_col else None

        # Get the appropirate event_date
        date_col = [s for s in col_names if date_re.search(s) is not None]
        date_q = resolve_date_query(date_col, col_names)

        queries_str = ',\n'.join([q for q in [race_q, sex_q, date_q, dob_q] if q is not None])
        q = f"""
        set role 'dojo-mh-role';
        drop table if exists {schema}.{tname};
        create table {schema}.{tname} as
            select client.joid,
            {queries_str},
            t.*
            --from {schema}.{tname} t
            from raw.{tname} t
            left join clean.jocojococlient client on client.sourceid = t.patid::varchar
            and client.source = 'JOCOJCMHCDEMOGRAPHICS.PATID';
        """
        db_conn.execute(q)
        db_conn.execute('COMMIT')

if __name__ == '__main__':
    clean_mhc_tables()
