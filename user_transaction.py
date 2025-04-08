from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, min, max, approx_count_distinct, date_format

# Initialize Spark session
spark = SparkSession.builder.appName("Rental_Analytics").getOrCreate()

input_path = "s3://vehicle-rental-marketplace/input-data/"


users_df = spark.read.csv(input_path + "users.csv", header=True, inferSchema=True)
locations_df = spark.read.csv(input_path + "locations.csv", header=True, inferSchema=True)
rentals_df = spark.read.csv(input_path + "rental_transactions.csv", header=True, inferSchema=True)
vehicles_df = spark.read.csv(input_path + "vehicles.csv", header=True, inferSchema=True)



rental_duration = rentals_df.withColumn("rental_hours", 
    (col("rental_end_time").cast("long") - col("rental_start_time").cast("long")) / 3600)

rental_duration_by_vehicle = rental_duration.join(vehicles_df, "vehicle_id", "inner") \
    .groupBy("vehicle_type").agg(avg("rental_hours").alias("avg_rental_hours"),
                                 min("rental_hours").alias("min_rental_hours"),
                                 max("rental_hours").alias("max_rental_hours"))



### ---------------- USER AND TRANSACTION METRICS ---------------- ###
# 1. Total Daily Transactions and Revenue
daily_transactions = rentals_df.withColumn("rental_date", date_format("rental_start_time", "yyyy-MM-dd")) \
    .groupBy("rental_date").agg(count("rental_id").alias("total_daily_transactions"),
                                sum("total_amount").alias("total_daily_revenue"))

# 2. Average Transaction Value
avg_transaction_value = rentals_df.agg(avg("total_amount").alias("avg_transaction_value"))

# 3. User Engagement Metrics (Total Transactions & Total Revenue Per User)
user_engagement = rentals_df.groupBy("user_id") \
    .agg(count("rental_id").alias("total_transactions"), sum("total_amount").alias("total_revenue")) \
    .join(users_df, "user_id", "left") \
    .select("first_name", "last_name", "email", "total_transactions", "total_revenue")

# 4. Max and Min Spending per User
user_spending = rentals_df.groupBy("user_id") \
    .agg(max("total_amount").alias("max_spent"), min("total_amount").alias("min_spent")) \
    .join(users_df, "user_id", "left") \
    .select("first_name", "last_name", "email", "max_spent", "min_spent")

# 5. Total Rental Hours per User
total_rental_hours_per_user = rental_duration.groupBy("user_id") \
    .agg(sum("rental_hours").alias("total_rental_hours")) \
    .join(users_df, "user_id", "left") \
    .select("first_name", "last_name", "email", "total_rental_hours")


### ---------------- SAVING METRICS TO S3 ---------------- ###
output_path = "s3://vehicle-rental-marketplace/output-data/location-vehicle/"

# Write DataFrames to S3 in Parquet format
daily_transactions.write.parquet(output_path + "daily_transactions", mode="overwrite")
avg_transaction_value.write.parquet(output_path + "avg_transaction_value", mode="overwrite")
user_engagement.write.parquet(output_path + "user_engagement", mode="overwrite")
user_spending.write.parquet(output_path + "user_spending", mode="overwrite")
total_rental_hours_per_user.write.parquet(output_path + "total_rental_hours_per_user", mode="overwrite")