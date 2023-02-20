import etltools.etltools as etl
import pytest
from pandas.testing import assert_frame_equal


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
    
    
    '''
    @pytest.fixture()
    def database(postgresql):
        with open('test_db.sql') as f:
            setup_sql = f.read()

        with postgresql.cursor() as cursor:
            cursor.execute(setup_sql)
            postgresql.commit()

        yield postgresql

    def test_example(database):
        #etl.load_data_to_db(df, table_name, engine)
        valid_df = etl.extract(TEST_CSV)
        #etl.load_data_to_db(valid_df, clean_historic_values_all_states, database)
        assert 1 == 1
    '''