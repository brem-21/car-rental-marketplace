from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import sys
from pyspark.sql.utils import AnalysisException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and return Spark session with error handling"""
    try:
        spark = SparkSession.builder \
            .appName("UserTransaction") \
            .getOrCreate()
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        sys.exit(1)

def read_csv_with_validation(spark, url, name):
    """Read CSV file with validation and error handling"""
    try:
        df = spark.read.csv(url, header=True, inferSchema=True)
        if df.rdd.isEmpty():
            logger.warning(f"Input DataFrame {name} is empty")
        logger.info(f"Successfully read {name} data from {url}")
        return df
    except AnalysisException as e:
        logger.error(f"File not found or invalid format for {name}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error reading {name}: {str(e)}")
        raise

def write_with_validation(df, path, name):
    """Write DataFrame with validation checks"""
    try:
        if df.rdd.isEmpty():
            logger.warning(f"Writing empty DataFrame for {name}")
        
        if not path.startswith("s3://"):
            logger.warning(f"Output path {path} doesn't look like a valid S3 path")
        
        df.write.mode("overwrite").parquet(path)
        logger.info(f"Successfully wrote {name} to {path}")
    except Exception as e:
        logger.error(f"Failed to write {name} to {path}: {str(e)}")
        raise

def calculate_rental_duration(df):
    """Calculate rental duration with error handling"""
    try:
        return df.withColumn(
            "rental_duration_hours",
            (col("rental_end_time").cast("long") - col("rental_start_time").cast("long")) / 3600
        )
    except Exception as e:
        logger.error(f"Failed to calculate rental duration: {str(e)}")
        raise

def main():
    try:
        # Initialize Spark
        spark = create_spark_session()
        
        # Define paths
        input_paths = {
            "locations": "s3://vehicle-rental-marketplace/input-data/locations.csv",
            "users": "s3://vehicle-rental-marketplace/input-data/users.csv",
            "rental": "s3://vehicle-rental-marketplace/input-data/rental_transactions.csv",
            "vehicles": "s3://vehicle-rental-marketplace/input-data/vehicles.csv"
        }
        
        output_path = "s3://vehicle-rental-marketplace/output-data/user_transactions"
        
        try:
            # Read input data
            locations = read_csv_with_validation(spark, input_paths["locations"], "locations")
            users = read_csv_with_validation(spark, input_paths["users"], "users")
            rental = read_csv_with_validation(spark, input_paths["rental"], "rental transactions")
            vehicles = read_csv_with_validation(spark, input_paths["vehicles"], "vehicles")
        except Exception as e:
            logger.error("Failed to read input data. Aborting processing.")
            raise
        
        try:
            # Calculate rental duration
            rental = calculate_rental_duration(rental)
        except Exception as e:
            logger.error("Failed in rental duration calculation")
            raise
        
        try:
            # Join rental transactions with user data
            user_rental = rental.join(users, rental.user_id == users.user_id, "inner")
            write_with_validation(user_rental, output_path, "user rental data")
        except Exception as e:
            logger.error("Failed in user-rental join operation")
            raise
        
        try:
            # Calculate daily transactions revenue
            daily_transactions_revenue = rental.withColumn(
                "transaction_date", to_date("rental_start_time")
            ).groupBy("transaction_date").agg(
                count("rental_id").alias("total_transactions"),
                sum("total_amount").alias("total_revenue")
            )
            write_with_validation(daily_transactions_revenue, output_path, "daily transactions revenue")
        except Exception as e:
            logger.error("Failed in daily transactions calculation")
            raise
        
        try:
            # Calculate average transaction amount
            avg_transaction = rental.agg(avg("total_amount").alias("average_transaction"))
            write_with_validation(avg_transaction, output_path, "average transaction")
        except Exception as e:
            logger.error("Failed in average transaction calculation")
            raise
        
        try:
            # Calculate user engagement metrics
            user_engagement = rental.groupby("user_id").agg(
                count("rental_id").alias("total_transactions"),
                sum("total_amount").alias("total_revenue")
            )
            write_with_validation(user_engagement, output_path, "user engagement metrics")
        except Exception as e:
            logger.error("Failed in user engagement calculation")
            raise
        
        try:
            # Calculate user spending metrics
            user_spending_metrics = rental.groupBy("user_id").agg(
                max("total_amount").alias("max_spent"),
                min("total_amount").alias("min_spent")
            )
            write_with_validation(user_spending_metrics, output_path, "user spending metrics")
        except Exception as e:
            logger.error("Failed in user spending metrics calculation")
            raise
        
        try:
            # Calculate total rental duration
            total_rental_duration = rental.groupby("user_id").agg(
                sum("rental_duration_hours").alias("total_rental_duration_hours")
            )
            write_with_validation(total_rental_duration, output_path, "total rental duration")
        except Exception as e:
            logger.error("Failed in rental duration calculation")
            raise
        
        logger.info("All processing completed successfully")
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        sys.exit(1)
    finally:
        try:
            spark.stop()
            logger.info("Spark session stopped successfully")
        except Exception as e:
            logger.error(f"Error while stopping Spark session: {str(e)}")

if __name__ == "__main__":
    main()