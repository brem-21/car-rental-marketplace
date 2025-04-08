from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("UserTransaction").getOrCreate()

location_url = "s3://vehicle-rental-marketplace/input-data/locations.csv"
user_url = "s3://vehicle-rental-marketplace/input-data/users.csv"
transaction_url = "s3://vehicle-rental-marketplace/input-data/rental_transactions.csv"
vehicle_url = "s3://vehicle-rental-marketplace/input-data/vehicles.csv"

output_path = "s3://vehicle-rental-marketplace/output-data/user_transactions"

# Read the CSV files from S3
locations = spark.read.csv(location_url, header=True, inferSchema=True)
users = spark.read.csv(user_url, header=True, inferSchema=True)
rental = spark.read.csv(transaction_url, header=True, inferSchema=True)
vehicles = spark.read.csv(vehicle_url, header=True, inferSchema=True)

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