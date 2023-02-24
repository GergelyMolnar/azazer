import csv
import json
import logging
import pathlib

import apache_beam as beam
import dateparser as dp
import pandas as pd
from apache_beam.io import iobase
from apache_beam.io.filebasedsource import FileBasedSource

# Set the log level
logging.getLogger().setLevel(logging.ERROR)


def get_csv_header(url_: str):
    """
    Get the header row to use as keys for the row-dictionaries
    """
    df_ = pd.read_csv(url_)
    csv_header_ = list(df_.columns)
    return csv_header_


def export_raw_csv(url_: str, export_path: str or pathlib.Path) -> None:
    df = pd.read_csv(url_)
    # export CSV file for debugging
    df.to_csv(export_path, header=True, index=False)


class MultiFileSource(FileBasedSource):
    """
    A custom source that reads multiple files with a wildcard
    """

    def __init__(self, file_pattern, *args, **kwargs):
        super().__init__(file_pattern=file_pattern, **kwargs)

    def split(self, desired_bundle_size=None, start_position=None, stop_position=None):
        # Override the split() method to return a list of file patterns
        file_patterns = self.file_pattern.split('*')
        return [iobase.SourceBundle(file_pattern_, start_position, stop_position)
                for file_pattern_ in file_patterns]


class JsonLinesWriter(beam.PTransform):
    ta_thresh = 20
    after_year = 2010

    def __init__(self, url_: str, header_: [str], file_path_prefix_: str, label_: str = None,
                 file_name_suffix_: str = None, num_shards_: int = None, shard_name_template_: str = None,
                 compression_type_: str = None):
        super().__init__(label=label_)
        self.url: str = url_
        self.csv_header_row: [str] = header_
        self.file_path_prefix: str = file_path_prefix_
        self.file_name_suffix: str = file_name_suffix_ or ''
        self.num_shards: int = num_shards_ or None
        self.shard_name_template: str = '-SSSSS-of-NNNNN' if shard_name_template_ else ''
        compr_types = ['auto', 'bzip2', 'gzip', 'uncompressed']
        compr_type_ = compression_type_ or ''
        self.compression_type: str = compr_type_ if compr_type_.lower().strip() in compr_types else compr_types[0]

    def _parse_csv(self, row_):
        row_ = next(csv.reader([row_]))
        if row_ != self.csv_header_row:
            return dict(zip(self.csv_header_row, row_))

    def _transaction_amount_to_float(self, row_):
        if 'transaction_amount' in row_.keys():
            row_['transaction_amount'] = float(row_['transaction_amount'])
        return row_

    def _add_date_column(self, row_):
        """
        Convert date string to datetime object and back to date string.
        """
        # avoiding issues potential caused by bad timestamp string by converting it to a datetime object
        row_['date'] = dp.parse(row_['timestamp']).strftime('%Y-%m-%d')
        return row_

    def _reduce_columns(self, row_):
        return row_['date'], row_['transaction_amount']

    def _sum_amounts(self, row_):
        key, values = row_
        total_amount = sum(value for value in values)
        return {'date': key, 'total_amount': total_amount}

    def expand(self, element):
        return (element
                | 'Read CSV data as TXT ' >> beam.io.ReadFromText(self.url, skip_header_lines=1)
                | 'Parse TXT to CSV ' >> beam.Map(self._parse_csv)
                | 'Convert STR to FLOAT ' >> beam.Map(self._transaction_amount_to_float)
                | f'Filter over {self.ta_thresh} ' >> beam.Filter(
                    lambda row: float(row['transaction_amount']) > self.ta_thresh)
                | 'Add date column' >> beam.Map(self._add_date_column)
                | f'Exclude before {self.after_year} ' >> beam.Filter(
                    lambda row: dp.parse(row['date']).year >= self.after_year)
                | 'Drop columns ' >> beam.Map(self._reduce_columns)
                | 'GroupBy date ' >> beam.GroupByKey()
                | 'Sum into new column ' >> beam.Map(self._sum_amounts)
                | 'Format json ' >> beam.Map(json.dumps)
                | 'Write to file ' >> beam.io.WriteToText(file_path_prefix=self.file_path_prefix,
                                                          file_name_suffix=self.file_name_suffix,
                                                          num_shards=self.num_shards,
                                                          shard_name_template=self.shard_name_template,
                                                          compression_type=self.compression_type,
                                                          ))


output_dir = pathlib.Path('output/')
output_file = 'results'
output_file_ext = '.jsonl.gz'

url = "gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv"
header = get_csv_header(url)
# only used for debugging
# export_raw_csv(url_=url, export_path=f"export/{url.split('//')[-1].split('/')[-1]}")


if __name__ == '__main__':
    # making sure the output path exists
    output_dir.mkdir(parents=True, exist_ok=True)

    with beam.Pipeline() as pipeline:
        data = (pipeline | JsonLinesWriter(url_=url,
                                           header_=header,
                                           file_path_prefix_=f"{output_dir / output_file}",
                                           file_name_suffix_=output_file_ext,
                                           num_shards_=1,
                                           compression_type_='gzip'))
        result = pipeline.run()
        result.wait_until_finish()
