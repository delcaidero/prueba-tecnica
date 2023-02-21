import etltools.etltools as etl
import pytest
from pandas.testing import assert_frame_equal

'''
Los test están pendientes :(
'''

TEST_CSV="./tests/test.csv"
URI_CSV="https://api.covidtracking.com/v1/states/daily.csv"

@pytest.mark.skip(reason="no es necesario testearlo siempre")
def test_extract_expected_column_number():
    """
    Comprobamos que extraemos el número esperado de columnas
    """
    valid_df = etl.extract(TEST_CSV)
    valid_n_rows, valid_n_columns = valid_df.shape
    uri_df = etl.extract(URI_CSV)
    uri_n_rows, uri_n_columns = valid_df.shape
    assert valid_n_columns == uri_n_columns

@pytest.mark.skip(reason="no es necesario testearlo siempre")
def test_extract_expected_columns():
    """
    Comprobamos que se contienen las columnas esperadas
    """
    valid_df = etl.extract(TEST_CSV)
    uri_df = etl.extract(URI_CSV)
    assert_frame_equal(valid_df.head(0), uri_df.head(0), check_dtype=False)
