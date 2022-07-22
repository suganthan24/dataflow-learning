import argparse
import json
import typing
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.combiners import CountCombineFn

class CommonLog (typing.NamedTuple):
    ip: str
    user_id: str
    lat: float
    lng: float
    timestamp: str
    http_request: str
    http_response: int
    num_bytes: int
    user_agent: str

class PerUserAggregation(typing.NamedTuple):
    user_id: str
    page_views: int
    total_bytes: int
    max_bytes: int
    min_bytes: int

def parse_json(element):
    row = json.loads(element)
    return CommonLog(**row)

def to_dict(element):
    return element._asdict()

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help="Input File Location")
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')

    opts = parser.parse_args()

    options = PipelineOptions()
    options.view_as(StandardOptions).runner = opts.runner


    p = beam.Pipeline(options=options)

    (p 
        | "Read Input Data" >> beam.io.ReadFromText(opts.input)
        | "Parse Json" >> beam.Map(parse_json).with_output_types(CommonLog)
        | "User Aggregation" >> beam.GroupBy('user_id')
                                    .aggregate_field('user_id', CountCombineFn(), 'page_views')
                                    .aggregate_field('num_bytes', sum, 'total_bytes')
                                    .aggregate_field('num_bytes', max, 'max_bytes')
                                    .aggregate_field('num_bytes', min, 'min_bytes')
                                    .with_output_types(PerUserAggregation)
        | "ToDict" >> beam.Map(lambda x: x._asdict())
        | "Print Output" >> beam.Map(print)
    )

    p.run()


if __name__ == '__main__':
    run()

"""
python3 main.py --input=../data/events.txt --runner=DirectRunner
"""