from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("UserTransaction").getOrCreate()

# Define schemas
rental_schema = StructType([
    StructField("rental_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("rental_start_time", TimestampType(), True),
    StructField("rental_end_time", TimestampType(), True),
    StructField("pickup_location", IntegerType(), True),
    StructField("dropoff_location", IntegerType(), True),
    StructField("total_amount", DoubleType(), True)
])

location_schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("location_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
])

user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("driver_license_number", StringType(), True),
    StructField("driver_license_expiry", DateType(), True),
    StructField("creation_date", DateType(), True),
    StructField("is_active", IntegerType(), True)
])

vehicle_schema = StructType([
    StructField("active", IntegerType(), True),
    StructField("vehicle_license_number", StringType(), True),
    StructField("registration_name", StringType(), True),
    StructField("license_type", StringType(), True),
    StructField("expiration_date", StringType(), True),
    StructField("permit_license_number", StringType(), True),
    StructField("certification_date", DateType(), True),
    StructField("vehicle_year", IntegerType(), True),
    StructField("base_telephone_number", StringType(), True),
    StructField("base_address", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("last_update_timestamp", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("vehicle_type", StringType(), True)
])

# File URLs
location_url = "s3://vehicle-rental-marketplace/input-data/locations.csv"
user_url = "s3://vehicle-rental-marketplace/input-data/users.csv"
transaction_url = "s3://vehicle-rental-marketplace/input-data/rental_transactions.csv"
vehicle_url = "s3://vehicle-rental-marketplace/input-data/vehicles.csv"
output_path = "s3://vehicle-rental-marketplace/output-data/user_transactions"

# Read CSV files with explicit schemas
locations = spark.read.csv(location_url, header=True, schema=location_schema)
users = spark.read.csv(user_url, header=True, schema=user_schema)
rental = spark.read.csv(transaction_url, header=True, schema=rental_schema)
vehicles = spark.read.csv(vehicle_url, header=True, schema=vehicle_schema)

# Calculate the rental duration in hours
# Assuming rental_start_time and rental_end_time are in string format
rental = rental.withColumn(
    "rental_duration_hours",
    (col("rental_end_time").cast("long") - col("rental_start_time").cast("long")) / 3600 # Enclose the subtraction within parentheses and divide the result by 3600
)

# Join the rental transactions with user data
user_rental = rental.join(users, rental.user_id == users.user_id, "inner")
user_rental.write.mode("overwrite").parquet(output_path)

# Calculate the total revenue per day for each total transaction
daily_transactions_revenue = rental.withColumn(
    "transaction_date", to_date("rental_start_time")
).groupBy("transaction_date").agg(
    count("rental_id").alias("total_transactions"),
    sum("total_amount").alias("total_revenue")
)

# write the daily transactions revenue to S3
daily_transactions_revenue.write.mode("overwrite").parquet(output_path)

# Average transaction amount 
avg_transaction = rental.agg(avg("total_amount").alias("average_transaction"))
avg_transaction.write.mode("overwrite").parquet(output_path)

# User engagements
user_engagement = rental.groupby("user_id").agg(
    count("rental_id").alias("total_transactions"),
    sum("total_amount").alias("total_revenue")
)
user_engagement.write.mode("overwrite").parquet(output_path)

#User spending by min, max
user_spending_metrics = rental.groupBy("user_id").agg(
    max("total_amount").alias("max_spent"),
    min("total_amount").alias("min_spent")
)

user_spending_metrics.write.mode("overwrite").parquet(output_path)

#user rental duration
total_rental_duration = rental.groupby("user_id").agg(sum("rental_duration_hours").alias("total_rental_duration_hours"))
total_rental_duration.write.mode("overwrite").parquet(output_path)

spark.stop()