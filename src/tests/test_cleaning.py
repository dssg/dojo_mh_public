""" Test that clean schema is correctly populated.
Part of this test is already implemented in sanity_checks.py
"""
import pytest
from utils.helpers import get_database_connection, get_all_table_names


@pytest.fixture
def db_conn():
    return get_database_connection()


def test_all_clean_or_semicleaned(db_conn):
    # Currently this tests intentionally fails
    raw = set(get_all_table_names(db_conn, schema='raw'))
    clean = set(get_all_table_names(db_conn, schema='clean'))
    ignore = set()  # tables to ignore; won't (semi-)clean
    # currently missing tables, i.e., output of
    # print((raw - ignore) - (clean | semi_clean))
    # {'joco110hsccclientaddress2', 'jocokdoccourtcase',
    # 'joco110hsccclientrelation2', 'jocokdocdemographics', 'jocokdocccsar',
    # 'joco110hsccclient2', 'jocopdarrestscharges', 'jocojimsprobdrugtest',
    # 'jocojcmhccalldetails', 'joco110hsccclientmisc2', 'jocojimsylsdata',
    # 'jocokdoccaseplangoals', 'jocopdcallsforservice', 'jocojimssectioncodes',
    # 'jocokdocstatus', 'jocokdocassessment', 'jocojimspretrialassessdata',
    # 'joco110hsccclientcontact2', 'jocoucmodeloutput', 'jocokdocdrugtesting',
    # 'jocokdocinterventions'}
    semi_clean = set(get_all_table_names(db_conn, schema='semi_clean'))
    print((raw - ignore) - (clean | semi_clean))
    assert (raw - ignore) == (clean | semi_clean)
