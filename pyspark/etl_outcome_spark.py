from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType, TimestampType

from datetime import datetime
import os


def extract_bills_from_application():
    spark = SparkSession.builder.appName(
        "GEMINI Data Processing").getOrCreate()

    extract_timestamp = F.current_timestamp()

    # Query last extract from Application Database
    last_timestamp_query = """
		SELECT updated_at
		FROM ( 
			SELECT bill_id, updated_at
			FROM bills_items 
			ORDER BY updated_at 
			DESC ) as bills_logs
		LIMIT 1
	"""

    result = spark.read.format("jdbc").options(
        url='jdbc:postgresql://localhost:5432/warehouse-analytic',
        driver='org.postgresql.Driver',
        query=last_timestamp_query,
        user='username',
        password='password'
    ).load()

    last_timestamp = datetime.min if result.rdd.isEmpty() else result.collect()[0].asDict()['updated_at']

    query = "SELECT * FROM bills as b \
		WHERE b.is_analyzed = 1 \
		AND b.bill_path IS NOT NULL \
		AND b.deleted_at IS NULL \
		AND b.updated_at > '{}' \
	".format(last_timestamp)

    # Query data from Application Database
    df = spark.read.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/appdb",
        driver="com.mysql.cj.jdbc.Driver",
        query=query,
        user="username",
        password="password"
    ).load()

    df = df.withColumn('extract_timestamp', extract_timestamp)

    # Save Extracted data into data lake
    df.write.parquet("data/extracted")


def convertType(strType):
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

    return type_dict[strType] if strType in type_dict else "none"


def common_bill_transform(df):
    df = df.filter(~ df.date.contains("undefined"))
    df = df.filter(~ df.date.contains("null"))
    df = df.withColumn(
        'date', F.regexp_replace('date', '2564', '2021'))
    df = df.withColumn(
        'date', F.regexp_replace('date', '2563', '2020'))
    df = df.withColumn(
        'date', F.regexp_replace('date', 'กันยายน', '9'))
    df = df.withColumn(
        'date', F.regexp_replace('date', 'สิงหาคม', '8'))
    df = df.withColumn(
        'date', F.regexp_replace('date', 'มิถุนายน', '6'))
    df = df.withColumn(
        'date', F.regexp_replace('date', 'กรกฏาคม', '7'))
    df = df.withColumn(
        'date', F.regexp_replace('date', 'มกราคม', '1'))
    df = df.withColumn('date', F.to_date(
        df.date, format="d-M-y"))

    return df


def transform_bill_items():
    spark = SparkSession.builder.appName(
        "GEMINI Data Processing").getOrCreate()
    sc = spark.sparkContext

    transform_timestamp = F.current_timestamp()

    extracted_df = spark.read.option(
        "mergeSchema", "true").parquet("data/extracted")

    item_schema = StructType([
        StructField('bill_id', IntegerType(), True),
        StructField('store_id', IntegerType(), True),
        StructField('date', DateType(), True),
        StructField('issuer', StringType(), True),
        StructField('name', StringType(), True),
        StructField('amount', DoubleType(), True),
        StructField('unit', StringType(), True),
        StructField('price', DoubleType(), True),
        StructField('total', DoubleType(), True),
        StructField('type', StringType(), True),
        StructField('updated_at', TimestampType(), True),
        StructField('extract_timestamp', TimestampType(), True),
    ])

    item_df = spark.createDataFrame(
        spark.sparkContext.emptyRDD(), schema=item_schema)

    convertTypeUDF = F.udf(lambda x: convertType(x), StringType())

    extracted_df = common_bill_transform(extracted_df)

    for row in extracted_df.rdd.toLocalIterator():
        temp_dict = {}

        bill_id = row['id']
        store_id = row['store_id']
        date = row['date']
        extract_timestamp = row['extract_timestamp']

        issuer = row['issuer'] if 'issuer' in row else ""
        updated_at = row['updated_at']

        if row['items'] == "[]":
            temp_dict = {}
            temp_dict['bill_id'] = bill_id
            temp_dict['store_id'] = store_id
            temp_dict['date'] = date
            temp_dict['issuer'] = issuer
            temp_dict['updated_at'] = updated_at

            temp_dict['name'] = issuer
            temp_dict['total'] = float(row['total_price'])

            temp_dict['amount'] = 1.0
            temp_dict['unit'] = ""
            temp_dict['price'] = 0.0
            temp_dict['type'] = ""

            temp_dict['extract_timestamp'] = extract_timestamp
            temp_df = spark.parallelize([temp_dict]).toDF(item_schema)
            item_df = item_df.union(temp_df)

        else:
            items = spark.read.json(sc.parallelize(
                [row['items']]), schema=item_schema)
            if 'total' in items.columns:
                items = items.withColumn(
                    'total', items['total'].cast(DoubleType()))

            if 'amount' in items.columns:
                items = items.withColumn(
                    'amount', items['amount'].cast(DoubleType()))

            for item in items.rdd.toLocalIterator():
                temp_dict = {}
                temp_dict['bill_id'] = bill_id
                temp_dict['store_id'] = store_id
                temp_dict['date'] = date
                temp_dict['issuer'] = issuer
                temp_dict['updated_at'] = updated_at

                temp_dict['amount'] = float(
                    item['amount']) if 'amount' in item and item['amount'] != None else 1.0
                temp_dict['name'] = item['name'] if item['name'] != None else ""
                temp_dict['unit'] = item['unit'] if item['unit'] != None else ""
                temp_dict['price'] = float(
                    item['price']) if item['price'] != None else 0.0
                temp_dict['type'] = item['type'] if item['type'] != None else ""

                if item['total'] != None:
                    temp_dict['total'] = float(item['total'])
                else:
                    temp_dict['total'] = float(
                        item['price']) if item['price'] != None else 0.0

                temp_dict['extract_timestamp'] = extract_timestamp
                temp_df = sc.parallelize([temp_dict]).toDF(item_schema)
                item_df = item_df.union(temp_df)

    item_df = item_df.withColumn('type', convertTypeUDF(F.col('type')))
    item_df = item_df.fillna('none', subset=['unit'])
    item_df = item_df.fillna(0)

    item_df = item_df.withColumn('transform_timestamp', transform_timestamp)

    item_df.write.parquet("data/transformed/bill_items")


def transform_into_outcome_pivot():
    spark = SparkSession.builder.appName(
        "GEMINI Data Processing").getOrCreate()

    process_time = F.current_timestamp()

    bill_items_df = spark.read.option(
        "mergeSchema", "true").parquet("data/transformed/bill_items")

    bill_items_df = bill_items_df.filter(
        bill_items_df['issuer'] != 'unknown').na.drop()

    pivot_df = bill_items_df.groupBy(
        "store_id", "date").pivot("type").sum("total")

    col_list = pivot_df.columns
    col_list.remove('date')
    col_list.remove('store_id')

    # Columns exist
    col_list_check = pivot_df.columns
    col_type = ['inventory', 'material', 'invesment', 'equipment',
                'operation', 'rent', 'employee', 'utilities', 'personal', 'none']

    for col in col_type:
        if col in col_list_check:
            pass
        else:
            pivot_df = pivot_df.withColumn(col, F.lit(0.0))

    pivot_df = pivot_df.fillna(0)
    pivot_df = pivot_df.withColumn(
        'total', sum([pivot_df[col] for col in col_list]))

    pivot_df = pivot_df.withColumn('process_timestamp', process_time)

    pivot_df.write.parquet("data/transformed/outcome_pivoted")


def load_into_bill_items():
    spark = SparkSession.builder.appName(
        "GEMINI Data Processing").getOrCreate()

    item_transformed_df = spark.read.option(
        "mergeSchema", "true").parquet("data/transformed/bill_items")

    item_transformed_df.write.format('jdbc').options(
        url='jdbc:postgresql://localhost:5432/gemini-analytic',
        driver='org.postgresql.Driver',
        dbtable='bills_items',
        user='username',
        password='password'
    ).mode('append').save()


def load_into_outcome_daily():
    spark = SparkSession.builder.appName(
        "GEMINI Data Processing").getOrCreate()

    pivot_transformed_df = spark.read.option(
        "mergeSchema", "true").parquet("data/transformed/outcome_pivoted")

    pivot_transformed_df.write.format('jdbc').options(
        url='jdbc:postgresql://localhost:5432/gemini-analytic',
        driver='org.postgresql.Driver',
        dbtable='outcome_daily',
        user='username',
        password='password'
    ).mode('append').save()


def pipeline():
    """
        Working Pipeline
        - extract_from_application
        - transform_data_from_application
            - load_into_bill_items
        - transform_into_outcome_pivot
            - load_into_outcome_daily

    """

    # Extract
    extract_bills_from_application()

    # Transform
    transform_bill_items()
    transform_into_outcome_pivot()

    # Load
    load_into_bill_items()
    load_into_outcome_daily()


if __name__ == '__main__':
    # OS Environment setup
    os.environ['PYSPARK_SUBMIT_ARGS'] = ' \
	--master local[*] \
	--packages org.postgresql:postgresql:42.3.0 \
	--jars /Users/pumrapee/tools/mysql-connector-java-8.0.26.jar, \
	pyspark-shell \ '

    pipeline()
