# Create the Options first


import google.auth
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
# from apache_beam.utils.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions

import apache_beam as beam
import time
from apache_beam.runners import DataflowRunner


options = PipelineOptions()

# Setting the Options Programmatically
options = PipelineOptions(flags=[])

# set the project to the default project in your current Google Cloud Environment
_, options.view_as(GoogleCloudOptions).project = google.auth.default()

# Sets the pipeline mode to streaming, so we can stream the data from PubSub.
options.view_as(StandardOptions).streaming = True
options.view_as(GoogleCloudOptions).enable_streaming_engine = True
# options.view_as(PipelineOptions).experiments = 'use_runner_v2' the log says it is by deafult

# number of workers
options.view_as(WorkerOptions).num_workers = 2

# Set the Google Cloud Region in which Cloud Dataflow run
options.view_as(GoogleCloudOptions).region = 'us-west1'

# Cloud Storage Location
dataflow_gcs_location = 'gs://gcp-dataeng-demos-soumya/dataflow'

# Dataflow Staging Location. This location is used to stage the Dataflow Pipeline and SDK binary
options.view_as(GoogleCloudOptions).staging_location = '{}/staging'.format(dataflow_gcs_location)
# Dataflow Temp Location. This location is used to store temporary files or intermediate results before finally outputting
options.view_as(GoogleCloudOptions).temp_location = '{}/temp'.format(dataflow_gcs_location)
# The directory to store the output files of the job
output_gcs_location = '{}/output'.format(dataflow_gcs_location)

########################################################################  With A Single Worker    ############################################################################


query = 'SELECT id,name FROM `mimetic-parity-378803.gcpdataset.my-table-customer-records`'


def insert_key(x):
    return (x['id'][0:6], (x['id'], x['name']))


def makeRow(x):
    row = dict(zip(('id', 'name'), x))
    return row


class make_api_call(beam.DoFn):

    def process(self, element):
        # importing the requests library
        import requests
        import json
        import urllib.request
        # from kafka import KafkaProducer

        # producer = KafkaProducer(bootstrap_servers=['34.28.118.32:9094'], api_version=(0, 10))

        # content = urllib.request.urlopen('http://35.239.64.67:32726/').read().decode('utf-8')
        # row = dict(zip(('message'),content))

        # The API endpoint
        # url = "http://35.239.64.67:32726/"
        url = "https://jsonplaceholder.typicode.com/posts/1"

        # A GET request to the API
        response = requests.get(url)

        # Print the response
        response_json = response.json()
        # producer.send('my-topic', json.dumps(response_json).encode('utf-8'))
        # if producer is not None:
        # producer.close()

        length = len(element[1])
        x = response_json['userId']
        x1 = response_json['id']
        x2 = response_json['title']
        x3 = response_json['body']
        # x=1
        # x1=1
        # x2='string'
        # x3='strong'

        row = dict(zip(('userId', 'id', 'title', 'body', 'length'), (x, x1, x2, x3, length)))
        yield row  # A return here was not working, please remember, from error message [row[0].json cannot be done], so the write to bigquery expects an interator


from apache_beam.io.gcp.bigquery import ReadFromBigQueryRequest, ReadAllFromBigQuery

import apache_beam as beam

p = beam.Pipeline(options=options)

# with beam.Pipeline(options=options) as p:
read_requests = (p |
                 beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
                 | beam.Map(insert_key)
                 | 'Group into batches' >> beam.GroupIntoBatches(50000)
                 | 'Make an External API Call' >> beam.ParDo(make_api_call())
                 | "Write to Bigquery" >> beam.io.Write(beam.io.WriteToBigQuery('my-table-message-second',
                                                                                dataset='gcpdataset',
                                                                                project='mimetic-parity-378803',
                                                                                schema='userId:INTEGER, id:INTEGER,title:STRING,body:STRING,length:INTEGER',
                                                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                                # If table does not exist before, this code throws error
                                                                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND

                                                                                )
                                                        )
                 )
# |beam.Map(print))

pipeline_result = DataflowRunner().run_pipeline(p, options=options)