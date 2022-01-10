import argparse
import logging
import json
import pyarrow

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import pymysql
import pymysql.cursors

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.io.gcp.internal.clients import bigquery

from datetime import datetime
process_time = datetime.now()


class QueryMySqlFn(beam.DoFn):

    def __init__(self, **server_configuration):
        self.config = server_configuration

    def start_bundle(self):
        self.mydb = pymysql.connect(
            cursorclass=pymysql.cursors.DictCursor, **self.config)

    def process(self, query):
        self.cursor = self.mydb.cursor()
        self.cursor.execute(query)
        for result in self.cursor:
            yield result


class ThaiDateCleaningDoFn(beam.DoFn):

    def process(self, element):

        element['date'] = element['date'].replace('2564', '2021')
        element['date'] = element['date'].replace('2563', '2020')
        element['date'] = element['date'].replace('กันยายน', '9')
        element['date'] = element['date'].replace('สิงหาคม', '8')
        element['date'] = element['date'].replace('มิถุนายน', '6')
        element['date'] = element['date'].replace('กรกฏาคม', '7')
        element['date'] = element['date'].replace('มกราคม', '1')
        # element['date'] = datetime.strptime(element['date'], "%d-%M-%y")

        yield element


class ItemsExtractingDoFn(beam.DoFn):

    def process(self, row):

        type_dict = {
            'วัตถุดิบ': 'inventory',
            'วัตถุดิบทางตรง': 'inventory',
            'วัตถุดิบทางอ้อม': 'inventory',
            'ทางตรง': 'inventory',
            'ทางอ้อม': 'inventory',
            'อุปกรณ์': 'equipment',
            'วัสดุ': 'material',
            'วัสดุใช้งาน': 'material',
            'อุปกรณ์ใช้งาน': 'equipment',
            'สาธารณูปโภค': 'utilities',
            'บริหาร': 'operation',
            'ส่วนตัว': 'personal',
            'ค่าเช่า': 'rent',
            'ค่าตอบแทนพนักงาน': 'employee'
        }

        bill_id = row['id']
        store_id = row['store_id']
        date = row['date']
        issuer = row['issuer'] if 'issuer' in row.keys() else ""
        updated_at = row['updated_at']
        extract_timestamp = row['extracted_timestamp']

        if row['items'] == "[]":
            item = {}

            item['bill_id'] = bill_id
            item['store_id'] = store_id
            item['date'] = date
            item['issuer'] = issuer
            item['updated_at'] = updated_at
            item['name'] = issuer
            item['total'] = float(row['total_price'])
            item['amount'] = 1.0
            item['unit'] = ""
            item['price'] = 0.0
            item['type'] = ""

            item['type'] = type_dict[item['type']
                                     ] if item['type'] in type_dict else "none"
            item['extracted_timestamp'] = extract_timestamp

            yield item

        else:
            items = json.loads(row['items'])

            for process_item in items:
                item = {}
                keys_list = process_item.keys()

                item['bill_id'] = bill_id
                item['store_id'] = store_id
                item['date'] = date
                item['issuer'] = issuer
                item['updated_at'] = updated_at
                item['amount'] = float(
                    process_item['amount']) if 'amount' in keys_list and process_item['amount'] != '' else 1.0
                item['name'] = process_item['name'] if process_item['name'] != None else ""
                item['unit'] = process_item['unit'] if 'unit' in keys_list else "none"
                item['price'] = float(
                    process_item['price']) if 'price' in keys_list else 0.0
                item['type'] = process_item['type'] if 'type' in keys_list else ""

                if 'total' in keys_list and process_item['total'] != '':
                    item['total'] = float(process_item['total'])
                else:
                    item['total'] = float(
                        process_item['price']) if 'price' in keys_list else 0.0

                item['type'] = type_dict[item['type']
                                         ] if item['type'] in type_dict else "none"
                item['extracted_timestamp'] = extract_timestamp

                yield item


def add_process_timestamp(element, column_name, process_time=datetime.now()):
    element[column_name] = process_time
    return element


def get_lastest_extracted():
    query = """
        	SELECT updated_at
        	FROM (
        		SELECT bill_id, updated_at
        		FROM bills_items
        		ORDER BY updated_at
        		DESC ) as bills_logs
        	LIMIT 1
    """

    engine = create_engine(
        'postgresql://pumrapee:test1234@localhost:5432/gemini-analytic')

    last_timestamp = pd.read_sql_query(query, con=engine)
    engine.dispose()

    if last_timestamp.empty:
        return datetime.min
    else:
        last_timestamp['updated_at'].values[0]
        return datetime.utcfromtimestamp(last_timestamp.tolist()/1e9)


def run(last_timestamp, argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    schema = pyarrow.schema([
        ('id', pyarrow.int64()),
        ('store_id', pyarrow.int64()),
        ('bill_path', pyarrow.string()),
        ('type', pyarrow.int64()),
        ('date', pyarrow.string()),
        ('issuer', pyarrow.string()),
        ('items', pyarrow.string()),
        ('price', pyarrow.float32()),
        ('discount_price', pyarrow.float32()),
        ('service_price', pyarrow.float32()),
        ('transport_price', pyarrow.float32()),
        ('vat_price', pyarrow.float32()),
        ('total_price', pyarrow.float32()),
        ('is_analyzed', pyarrow.int64()),
        ('created_at', pyarrow.date64()),
        ('updated_at', pyarrow.date64()),
        ('deleted_at', pyarrow.date64()),
        ('extracted_timestamp', pyarrow.date64())
    ])

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:

        query = "SELECT * FROM bills as b \
        	WHERE b.is_analyzed = 1 \
        	AND b.bill_path IS NOT NULL \
        	AND b.deleted_at IS NULL \
        	AND b.updated_at > '{}' \
        ".format(last_timestamp)

        # Read data from Application Database.
        extracted_lines = (
            p
            | 'CreateQuery' >> beam.Create([query])
            | 'ReadFromAppDB' >> beam.ParDo(
                QueryMySqlFn(),
                host="test",
                user="user",
                password="password1234"
            )
            | 'AddExtractedTime' >> beam.Map(lambda x: add_process_timestamp(x, column_name="extracted_timestamp"))
        )

        # Save Original data from DB
        extracted_lines | 'SaveExtractedData' >> WriteToParquet(
            file_path_prefix="gs://gcp-analytic/extracted/bills/{}.parquet".format(
                process_time),
            schema=schema
        )

        # Extract items from each records.
        items_lines = (
            extracted_lines
            | 'DateFilter' >> (beam.Filter(
                lambda element: "undefined" not in element['date'] and "null" not in element['date'])
            )
            | 'ThaiDateCleansing' >> (beam.ParDo(ThaiDateCleaningDoFn()))
            | 'ExtractItems' >> (
                beam.ParDo(ItemsExtractingDoFn()))
            | 'AddTransformTime' >> beam.Map(lambda x: add_process_timestamp(x, column_name="transform_timestamp"))
        )

        table_spec = bigquery.TableReference(
            projectId='gcp-analytic',
            datasetId='records',
            tableId='record_items')

        table_schema = 'bill_id:INTEGER, store_id:INTEGER, date:STRING, issuer:STRING, name:STRING, amount:FLOAT, unit:STRING, price:FLOAT, total:FLOAT, type:STRING, updated_at:TIMESTAMP, extracted_timestamp:TIMESTAMP, transform_timestamp:TIMESTAMP'

        items_lines | 'LoadBillItemsIntoWarehouse' >> beam.io.WriteToBigQuery(
            table_spec,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    last_timestamp = get_lastest_extracted()

    run(last_timestamp)
