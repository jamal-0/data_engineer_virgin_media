import apache_beam as beam
from datetime import datetime
import json

def Split(element):
  return [element.split(',')]


def filter_more_than_20(element):
  return float(element[-1]) > 20


def filter_after_2010(element):
  dt = element[0][:-4]
  dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
  year_2010 = datetime(2010, 1, 1)
  return dt >= year_2010


def create_key_value_pair(element):
  dt = element[0][:-4]
  dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S').date()
  dt = dt.replace(month=1, day=1)
  return (dt.strftime('%Y-%m-%d'), float(element[-1]))


def convert_to_object(element):
  return {element[0]:element[1]}


class CompositeTransform(beam.PTransform):

  def expand(self, input_data):

    return (
        input_data
                | "filter the transactions" >> beam.Filter(filter_more_than_20)
                | "filter the date" >> beam.Filter(filter_after_2010)
                | "create key values" >> beam.Map(create_key_value_pair)
                | "sum on key(year)" >> beam.CombinePerKey(lambda values: sum(values))
                | "convert to object" >> beam.Map(convert_to_object)

    )

with beam.Pipeline() as pipeline:
  input_data = (pipeline
                | "read from csv">> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=True)
                | "split the rows" >> beam.ParDo(Split)
  )

  filtering_data = (input_data
                | "composite transform" >> CompositeTransform()

  )

  output_data = (filtering_data
                | "to json" >> beam.Map(json.dumps)
                | "write" >> beam.io.WriteToText("output/results", file_name_suffix=".jsonl.gz", compression_type='gzip',  header='date, total_amount', shard_name_template='')
  ) 
  