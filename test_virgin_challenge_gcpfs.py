from unittest.mock import patch

import pandas as pd

from virgin_challenge import get_csv_header

# test data
exp_header_row = ['timestamp', 'origin', 'destination', 'transaction_amount']
df_data = {
    'timestamp': ['2022-02-23 12:00:00 UTC', '2022-02-24 01:01:00 UTC', '2022-02-24 23:23:00 UTC'],
    'origin': ['132456', '789456', '789457'],
    'destination': ['abcdef', 'poiuyl', 'poiuyk'],
    'transaction_amount': ['10.0', '5.0', '5.0']
}
sample_csv_data = pd.DataFrame(df_data, columns=exp_header_row)

url = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
bad_url = 'gs://there/is/no/data/for/transactions.csv'


def test_get_csv_header():
    with patch('pandas.read_csv') as mock_read_csv:
        mock_read_csv.return_value = sample_csv_data
        header_row = get_csv_header(url)
        assert header_row == exp_header_row
