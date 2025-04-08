import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, min, max, approx_count_distinct

# Set up logging
logging.basicConfig(
    level=logging.INFO,  # You can change the level to DEBUG, ERROR, etc.
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Spark session
spark = SparkSession.builder.appName("LocationVehicle").getOrCreate()

# Input path from S3
input_path = "s3://vehicle-rental-marketplace/input-data/"

# Read data from S3
logging.info(f"Reading data from {input_path}")
users_df = spark.read.csv(input_path + "users.csv", header=True, inferSchema=True)
locations_df = spark.read.csv(input_path + "locations.csv", header=True, inferSchema=True)
rentals_df = spark.read.csv(input_path + "rental_transactions.csv", header=True, inferSchema=True)
vehicles_df = spark.read.csv(input_path + "vehicles.csv", header=True, inferSchema=True)

# 1. Revenue per Location
logging.info("Calculating revenue per location...")
revenue_per_location = rentals_df.groupBy("pickup_location") \
    .agg(sum("total_amount").alias("total_revenue")) \
    .join(locations_df, rentals_df.pickup_location == locations_df.location_id, "left") \
    .select("location_name", "total_revenue", "location_id")

# 2. Total Transactions per Location
logging.info("Calculating total transactions per location...")
transactions_per_location = rentals_df.groupBy("pickup_location") \
    .agg(count("rental_id").alias("total_transactions")) \
    .join(locations_df, rentals_df.pickup_location == locations_df.location_id, "left") \
    .select("location_name", "total_transactions")

# 3. Average Transaction Amount per Location
logging.info("Calculating average transaction amount per location...")
avg_transaction_per_location = rentals_df.groupBy("pickup_location") \
    .agg(avg("total_amount").alias("avg_transaction_amount")) \
    .join(locations_df, rentals_df.pickup_location == locations_df.location_id, "left") \
    .select("location_name", "avg_transaction_amount")

# 4. Max/Min Transaction Amount per Location
logging.info("Calculating max/min transaction amount per location...")
max_min_transaction_per_location = rentals_df.groupBy("pickup_location") \
    .agg(max("total_amount").alias("max_transaction"), min("total_amount").alias("min_transaction")) \
    .join(locations_df, rentals_df.pickup_location == locations_df.location_id, "left") \
    .select("location_name", "max_transaction", "min_transaction")

# 5. Unique Vehicles Used per Location
logging.info("Calculating unique vehicles per location...")
unique_vehicles_per_location = rentals_df.groupBy("pickup_location") \
    .agg(approx_count_distinct("vehicle_id").alias("unique_vehicles")) \
    .join(locations_df, rentals_df.pickup_location == locations_df.location_id, "left") \
    .select("location_name", "unique_vehicles")

# 6. Rental Duration Metrics by Vehicle Type
logging.info("Calculating rental duration metrics by vehicle type...")
rental_duration = rentals_df.withColumn("rental_hours", 
    (col("rental_end_time").cast("long") - col("rental_start_time").cast("long")) / 3600)

rental_duration_by_vehicle = rental_duration.join(vehicles_df, "vehicle_id", "inner") \
    .groupBy("vehicle_type").agg(avg("rental_hours").alias("avg_rental_hours"),
                                 min("rental_hours").alias("min_rental_hours"),
                                 max("rental_hours").alias("max_rental_hours"))

# Output path for processed data in S3
base_output_path = "s3://vehicle-rental-marketplace/output-data/user_transactions"

# Log the output paths before writing
logging.info(f"Writing processed data to {base_output_path}")

# Write each DataFrame to S3 in Parquet format
logging.info(f"Writing revenue per location to {base_output_path}revenue_per_location")
revenue_per_location.write.parquet(base_output_path + "revenue_per_location", mode="overwrite")

logging.info(f"Writing transactions per location to {base_output_path}transactions_per_location")
transactions_per_location.write.parquet(base_output_path + "transactions_per_location", mode="overwrite")

logging.info(f"Writing avg transaction per location to {base_output_path}avg_transaction_per_location")
avg_transaction_per_location.write.parquet(base_output_path + "avg_transaction_per_location", mode="overwrite")

logging.info(f"Writing max/min transaction per location to {base_output_path}max_min_transaction_per_location")
max_min_transaction_per_location.write.parquet(base_output_path + "max_min_transaction_per_location", mode="overwrite")

logging.info(f"Writing unique vehicles per location to {base_output_path}unique_vehicles_per_location")
unique_vehicles_per_location.write.parquet(base_output_path + "unique_vehicles_per_location", mode="overwrite")

logging.info(f"Writing rental duration by vehicle to {base_output_path}rental_duration_by_vehicle")
rental_duration_by_vehicle.write.parquet(base_output_path + "rental_duration_by_vehicle", mode="overwrite")

logging.info("Data successfully written to S3.")