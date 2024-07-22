import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from google.cloud import pubsub_v1, storage, bigquery, logging
from google.cloud.logging import Client as LoggingClient
from google.cloud.logging_v2.handlers import CloudLoggingHandler
import json
from apache_beam.transforms.window import FixedWindows
import datetime
# Configuration
PROJECT_ID = 'PROJECTID'
BUCKET_NAME = 'pestotech'
DATASET_ID = 'advertisex_dataset'
TABLE_ID = 'ad_events_record'
SUBSCRIPTIONS = {
    'ad_impressions': 'projects/PROJECTID/subscriptions/ad-impressions-sub',
    'clicks': 'projects/PROJECTID/subscriptions/clicks-sub',
    'conversions': 'projects/PROJECTID/subscriptions/conversions-sub',
    'bid_requests': 'projects/PROJECTID/subscriptions/bid-requests-sub',
}

# Initialize clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()
logging_client = LoggingClient()
handler = CloudLoggingHandler(logging_client)
handler.setLevel(level='INFO')

# Error handling and monitoring
def log_error(error_message):
    logging.error(error_message)

# Data Ingestion (Pub/Sub)
class ParsePubSubMessage(beam.DoFn):
    def process(self, element):
        message = element.decode('utf-8')
        print(message)
        try:
            record = json.loads(message)
            print(record)
            return [record]
        except json.JSONDecodeError as e:
            log_error(f"JSONDecodeError: {e}")
            return []

# Data Processing
class StandardizeAndEnrich(beam.DoFn):
    def process(self, element, event_type):
        element['event_type'] = event_type
        print("added event type")
        yield element

class FormatForBigQuery(beam.DoFn):
    def process(self, element):
        print("inside correlation")
        user_id, event_dict = element

        # Debugging statement
        print(f"Processing user_id: {user_id}")
        print(f"Event dictionary: {event_dict}")
        impressions = [item for sublist in event_dict.get('ad_impressions', []) for item in sublist.get('ad_impressions', [])]
        clicks = [item for sublist in event_dict.get('clicks', []) for item in sublist.get('clicks', [])]
        conversions = [item for sublist in event_dict.get('conversions', []) for item in sublist.get('conversions', [])]
        bid_requests = [item for sublist in event_dict.get('bid_requests', []) for item in sublist.get('bid_requests', [])]

        # Convert Unix timestamps to BigQuery TIMESTAMP format
        def unix_to_timestamp(unix_timestamp):
            return datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')

        correlated_records = [{
            'userid': user_id,
            'impressions': [{'ad_creative_id': i.get('ad_creative_id'),
                            'timestamp': unix_to_timestamp(i.get('timestamp')),
                            'website': i.get('website')} for i in impressions],
            'clicks': [{'ad_campaign_id': i.get('ad_campaign_id'),
                        'timestamp': unix_to_timestamp(i.get('timestamp')),
                        'conversion_type': i.get('conversion_type')} for i in clicks],
            'conversions': [{'ad_campaign_id': i.get('ad_campaign_id'),
                             'timestamp': unix_to_timestamp(i.get('timestamp')),
                             'conversion_type': i.get('conversion_type')} for i in conversions],
            'bidrequests': [{'auction_id': i.get('auction_id'),
                             'timestamp': unix_to_timestamp(i.get('timestamp')),
                             'bid_amount': i.get('bid_amount')} for i in bid_requests]
        }]

        print(correlated_records)
        return correlated_records

# Data Storage (BigQuery)
def write_to_bigquery(element):
    try:
        table_ref = bigquery_client.dataset(DATASET_ID).table(TABLE_ID)
        print(element)
        print("inserting")
        errors = bigquery_client.insert_rows_json(table_ref, [element])
        if errors:
            print(f"BigQuery insert errors: {errors}")
    except Exception as e:
        print(f"BigQuery error: {e}")

# Pipeline Options
options = PipelineOptions(
    streaming=True,
    project=PROJECT_ID,
    runner='DirectRunner',
    temp_location=f'gs://{BUCKET_NAME}/temp',
    region="us-central1",
    max_num_workers=1
)
gcp_options = options.view_as(GoogleCloudOptions)
gcp_options.job_name = 'advertisex-data-pipeline'
# Dataflow Pipeline
with beam.Pipeline(options=options) as pipeline:
    window_size = 10   # 10 sec window

    ad_impressions = (pipeline
                      | "Read Ad Impressions" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTIONS['ad_impressions'])
                      | "Parse Ad Impressions" >> beam.ParDo(ParsePubSubMessage())
                      | "Standardize Ad Impressions" >> beam.ParDo(StandardizeAndEnrich(), event_type='impression')
                      | "Add Key for Impressions" >> beam.Map(lambda record: (record['user_id'], {'ad_impressions': [record]}))
                      | "Window Impressions" >> beam.WindowInto(FixedWindows(window_size)))

    clicks = (pipeline
              | "Read Clicks" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTIONS['clicks'])
              | "Parse Clicks" >> beam.ParDo(ParsePubSubMessage())
              | "Standardize Clicks" >> beam.ParDo(StandardizeAndEnrich(), event_type='click')
              | "Add Key for Clicks" >> beam.Map(lambda record: (record['user_id'], {'clicks': [record]}))
              | "Window Clicks" >> beam.WindowInto(FixedWindows(window_size)))

    conversions = (pipeline
                   | "Read Conversions" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTIONS['conversions'])
                   | "Parse Conversions" >> beam.ParDo(ParsePubSubMessage())
                   | "Standardize Conversions" >> beam.ParDo(StandardizeAndEnrich(), event_type='conversion')
                   | "Add Key for Conversions" >> beam.Map(lambda record: (record['user_id'], {'conversions': [record]}))
                   | "Window Conversions" >> beam.WindowInto(FixedWindows(window_size)))

    bid_requests = (pipeline
                    | "Read Bid Requests" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTIONS['bid_requests'])
                    | "Parse Bid Requests" >> beam.ParDo(ParsePubSubMessage())
                    | "Standardize Bid Requests" >> beam.ParDo(StandardizeAndEnrich(), event_type='bid_request')
                    | "Add Key for Bid Requests" >> beam.Map(lambda record: (record['user_id'], {'bid_requests': [record]}))
                    | "Window Bid Requests" >> beam.WindowInto(FixedWindows(window_size)))

    # Group all events by user_id
    grouped_events = (({'ad_impressions': ad_impressions, 'clicks': clicks, 'conversions': conversions, 'bid_requests': bid_requests})
                      | "CoGroup by Key" >> beam.CoGroupByKey()
                      | "Format for BigQuery" >> beam.ParDo(FormatForBigQuery()))

    grouped_events | "Write to BigQuery" >> beam.Map(write_to_bigquery)

# Batch Ingestion (GCS to BigQuery via Dataflow)
def run_batch_ingestion(file_path, table_id):
    with beam.Pipeline(options=PipelineOptions()) as p:
        if file_path.endswith('.csv'):
            events = (p | 'Read CSV' >> beam.io.ReadFromText(file_path, skip_header_lines=1)
                        | 'Parse CSV' >> beam.Map(lambda line: dict(zip(
                            ['timestamp', 'user_id', 'ad_campaign_id', 'conversion_type'],
                            line.split(',')
                        ))))
        elif file_path.endswith('.avro'):
            events = (p | 'Read Avro' >> beam.io.ReadFromAvro(file_path))
        
        events | 'Write to BigQuery' >> beam.Map(write_to_bigquery)

# Example of running batch ingestion
# run_batch_ingestion('gs://your-gcs-bucket-name/path/to/clicks.csv', 'clicks_table')
# run_batch_ingestion('gs://your-gcs-bucket-name/path/to/conversions.avro', 'conversions_table')
