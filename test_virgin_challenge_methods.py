import unittest

from virgin_challenge import JsonLinesWriter


class TestJsonLinesWriter(unittest.TestCase):

    def setUp(self):
        self.url = 'gs://test_url'
        self.header = ['timestamp', 'origin', 'destination', 'transaction_amount']
        self.file_path_prefix = 'test_prefix'
        self.json_lines_writer = JsonLinesWriter(url_=self.url,
                                                 header_=self.header,
                                                 file_path_prefix_=self.file_path_prefix)

    def test_parse_csv(self):
        row = '2022-02-23,origin,destination,100'
        expected_result = {
            'timestamp': '2022-02-23',
            'origin': 'origin',
            'destination': 'destination',
            'transaction_amount': '100'
        }
        self.assertEqual(self.json_lines_writer._parse_csv(row), expected_result)

    def test_transaction_amount_to_float(self):
        row = {
            'timestamp': '2022-02-23',
            'origin': 'origin',
            'destination': 'destination',
            'transaction_amount': '100'
        }
        expected_result = {
            'timestamp': '2022-02-23',
            'origin': 'origin',
            'destination': 'destination',
            'transaction_amount': 100.0
        }
        self.assertEqual(self.json_lines_writer._transaction_amount_to_float(row), expected_result)

    def test_add_date_column(self):
        row = {
            'timestamp': '2022-02-23T12:34:56Z',
            'origin': 'origin',
            'destination': 'destination',
            'transaction_amount': 100.0
        }
        expected_result = {
            'timestamp': '2022-02-23T12:34:56Z',
            'origin': 'origin',
            'destination': 'destination',
            'transaction_amount': 100.0,
            'date': '2022-02-23'
        }
        self.assertEqual(self.json_lines_writer._add_date_column(row), expected_result)

    def test_reduce_columns(self):
        row = {
            "timestamp": "2020-01-01 00:00:00",
            "origin": "origin",
            "destination": "destination",
            "transaction_amount": 50.0,
            "date": "2020-01-01",
        }
        expected_result = ("2020-01-01", 50.0)
        self.assertEqual(self.json_lines_writer._reduce_columns(row), expected_result)

    def test_sum_amounts(self):
        row = ("2020-01-01", [50.0, 100.0])
        expected_result = {"date": "2020-01-01", "total_amount": 150.0}
        self.assertEqual(self.json_lines_writer._sum_amounts(row), expected_result)
