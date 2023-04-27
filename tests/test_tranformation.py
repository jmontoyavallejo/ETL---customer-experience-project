import pytest
from unittest import mock
from google.cloud import bigquery
from app.modules.Tranformation import Big_query_executor

@pytest.fixture(scope='module')
def big_query_executor():
    return Big_query_executor('customer-experience-384423')

def test_execute_query(big_query_executor):
    query = 'SELECT * FROM `customer-experience-384423.Data_cruda.mineria_resultado_campanas` LIMIT 10'
    with mock.patch.object(bigquery.Client, 'query') as mock_query:
        mock_query.return_value.result.return_value = None
        big_query_executor.execute_query(query)
        mock_query.assert_called_once_with(query)