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


# JOin the rental transactions with vehicle data
rental_vehicle_df = rental.join(vehicles, "vehicle_id", "left")
rental_vehicle_df.write.mode("overwrite").parquet(output_path)

# Join the rental_vehicle transactions with location data
rental_vehicle_location_df = rental_vehicle_df.join(
    locations, rental_vehicle_df["pickup_location"] == locations["location_id"], "left"
)
rental_vehicle_location_df.write.mode("overwrite").parquet(output_path)

# calculate the revenue per location
revenue_per_location = rental.groupBy("pickup_location").agg(
    sum("total_amount").alias("total_revenue")
).join(
    locations, rental["pickup_location"] == locations["location_id"], "left"
).select(
    col("location_id"),
    col("location_name"),
    col("total_revenue")
)
revenue_per_location.write.mode("overwrite").parquet(output_path)

# Compute total transactions per location (pickup_location)
transactions_per_location = rental.groupBy("pickup_location").agg(
    count("rental_id").alias("total_transactions")
).join(
    locations, rental["pickup_location"] == locations["location_id"], "left"
).select(
    col("location_id"),
    col("location_name"),
    col("total_transactions")
)
transactions_per_location.write.mode("overwrite").parquet(output_path)

# Compute average transaction amount per location (pickup_location)
avg_transaction_per_location = rental.groupBy("pickup_location").agg(
    avg("total_amount").alias("avg_transaction_amount")
).join(
    locations, rental["pickup_location"] == locations["location_id"], "left"
).select(
    col("location_id"),
    col("location_name"),
    col("avg_transaction_amount")
)
avg_transaction_per_location.write.mode("overwrite").parquet(output_path)

# Compute max and min transaction amount per location (pickup_location)
max_min_transaction_per_location = rental.groupBy("pickup_location").agg(
    max("total_amount").alias("max_transaction_amount"),
    min("total_amount").alias("min_transaction_amount")
).join(
    locations, rental["pickup_location"] == locations["location_id"], "left"
).select(
    col("location_id"),
    col("location_name"),
    col("max_transaction_amount"),
    col("min_transaction_amount")
)
max_min_transaction_per_location.write.mode("overwrite").parquet(output_path)


# Compute unique vehicles used per location (pickup_location)
unique_vehicles_per_location = rental.groupBy("pickup_location").agg(
    countDistinct("vehicle_id").alias("unique_vehicles_used")
).join(
    locations, rental["pickup_location"] == locations["location_id"], "left"
).select(
    col("location_id"),
    col("location_name"),
    col("unique_vehicles_used")
)
unique_vehicles_per_location.write.mode("overwrite").parquet(output_path)


# calculate rental duration metrics
rental_duration_metrics_by_vehicle_type = rental_vehicle_df.groupBy("vehicle_type").agg(
    avg("rental_duration_hours").alias("avg_rental_duration"),
    max("rental_duration_hours").alias("max_rental_duration"),
    min("rental_duration_hours").alias("min_rental_duration"),
    sum("rental_duration_hours").alias("total_rental_duration")
)
rental_duration_metrics_by_vehicle_type.write.mode("overwrite").parquet(output_path)
spark.stop()