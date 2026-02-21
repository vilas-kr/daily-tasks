from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import subprocess

# Create session
spark = SparkSession.builder \
    .appName('E commerce analysis') \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# -------------------------------------------------------------------------
# Step 1: Upload Data to HDFS
# -------------------------------------------------------------------------

# Create directory inside HDFS
command = 'hdfs dfs -mkdir -p /ecommerce/raw'
subprocess.run(command, shell=True)

# upload orders.csv to HDFS
command = f'hdfs dfs -put archive/olist_order_items_dataset.csv /ecommerce/raw'
subprocess.run(command, shell=True)

# upload orders_dataset.csv to HDFS
command = f'hdfs dfs -put archive/olist_orders_dataset.csv /ecommerce/raw'
subprocess.run(command, shell=True)

# -------------------------------------------------------------------------
# Step 2: Read Data in PySpark
# -------------------------------------------------------------------------

# Read data from HDFS
PATH = 'hdfs:///ecommerce/'
orders = spark.read \
    .option('header', True) \
    .option('inferSchema', True) \
    .csv(PATH + 'raw/olist_orders_dataset.csv')

order_items = spark.read \
    .option('header', True) \
    .option('inferSchema', True) \
    .csv(PATH + 'raw/olist_order_items_dataset.csv')

# Print Schema
print('\nOrders Schema :')
orders.printSchema()

print('\nOrders item schema :')
order_items.printSchema()

# Count records in both dataframe
print(f'\nOrders dataframe {orders.count()} records')
print(f'\nOrders item dataframe {order_items.count()} records')

# -------------------------------------------------------------------------
# Step 3: Join Data
# -------------------------------------------------------------------------

# Joining both dataset
total_orders = orders.join(order_items,
        'order_id',
        'inner')

# handling null values
total_orders = total_orders.na.drop(subset=['order_id', 'order_item_id', 'product_id',
                                    'order_delivered_customer_date'])
total_orders = total_orders.na.fill({
        'order_status' : 'unknown',
        'seller_id' : 'unknown',
    })

# Convert date columns to proper format
total_orders = total_orders.withColumn(
        'order_purchase_timestamp',
        to_timestamp(col('order_purchase_timestamp'),
            'yyyy-MM-dd HH:mm:ss')
    ).withColumn(
        'order_delivered_carrier_date',
        to_timestamp(col('order_delivered_carrier_date'),
            'yyyy-MM-dd HH:mm:ss')
    ).withColumn(
        'order_delivered_customer_date',
        to_timestamp(col('order_delivered_customer_date'),
            'yyyy-MM-dd HH:mm:ss')
    ).withColumn(
        'order_estimated_delivery_date',
        to_timestamp(col('order_estimated_delivery_date'),
            'yyyy-MM-dd HH:mm:ss')
    ).withColumn(
        'shipping_limit_date',
        to_timestamp(col('shipping_limit_date'),
            'yyyy-MM-dd HH:mm:ss')
    ).withColumn(
        'order_approved_at',
        to_timestamp(col('order_approved_at'),
            'yyyy-MM-dd HH:mm:ss')
    )

# -------------------------------------------------------------------------
# Step 4: performance analytics
# -------------------------------------------------------------------------

# Calculate total revenue
total_revenue = total_orders.filter(
        col('order_status') == 'delivered'
    ).agg(
        sum('price').alias('total_revenue')
    ).collect()[0]

print(f'\nTotal Revenue is : {total_revenue['total_revenue']}')

# Calculate monthly revenue
monthly_revenue = total_orders.filter(
        col('order_status') == 'delivered'
    ).withColumn(
        'month',
        month(col('order_delivered_customer_date'))
    ).withColumn(
        'year',
        year(col('order_delivered_customer_date'))
    ).groupBy('year', 'month').agg(
        sum(col('price')).alias('total_revenue')
    )
    
print('\nMonthly Revenue :')
monthly_revenue.show()

# Top 5 selling products
top_selling_products = total_orders.filter(
        col('order_status') == 'delivered'
    ).groupBy('product_id').agg(
        count(col('order_id')).alias('Total_orders')
    ).sort(col('Total_orders').desc())

print('\nTop 5 selling products')
top_selling_products.show(5, False)

# Average order value
avg_order_value = total_orders.agg(
        avg('price').alias('avg_revenue')
    ).collect()[0]
print(f'\nAverage order value is : {avg_order_value['avg_revenue']}')

# -------------------------------------------------------------------------
# Step 5 : Save output
# -------------------------------------------------------------------------

# Create analytics
command = 'hdfs dfs -mkdir -p /ecommerce/analytics'
subprocess.run(command, shell=True)

# Save output to HDFS in parquet format
total_orders.write\
    .mode('overwrite') \
    .parquet(PATH + 'analytics/total_orders')

monthly_revenue.write\
    .mode('overwrite') \
    .parquet(PATH + 'analytics/monthly_revenue')

top_selling_products.write\
    .mode('overwrite') \
    .parquet(PATH + 'analytics/top_selling_products')

print('\nUploaded result dataframe to HDFS')




