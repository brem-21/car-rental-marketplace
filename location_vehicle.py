from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
from pyspark.sql.utils import AnalysisException
import logging

def setup_logging():
    """Configure logging for error handling"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

logger = setup_logging()

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

def read_csv_with_retry(spark, url, name, retries=3):
    """Read CSV file with retry logic"""
    for attempt in range(retries):
        try:
            df = spark.read.csv(url, header=True, inferSchema=True)
            logger.info(f"Successfully read {name} data from {url}")
            return df
        except AnalysisException as e:
            logger.warning(f"Attempt {attempt + 1} failed for {name}: {str(e)}")
            if attempt == retries - 1:
                logger.error(f"Failed to read {name} after {retries} attempts")
                raise
        except Exception as e:
            logger.error(f"Unexpected error reading {name}: {str(e)}")
            raise

def write_parquet_with_checks(df, path, name):
    """Write DataFrame to Parquet with validation"""
    try:
        # Validate DataFrame is not empty
        if df.rdd.isEmpty():
            logger.warning(f"{name} DataFrame is empty. Proceeding with write anyway.")
        
        # Check if output path looks valid
        if not path.startswith("s3://"):
            logger.warning(f"Output path {path} doesn't look like a valid S3 path")
        
        df.write.mode("overwrite").parquet(path)
        logger.info(f"Successfully wrote {name} to {path}")
    except Exception as e:
        logger.error(f"Failed to write {name} to {path}: {str(e)}")
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
        
        output_path = "s3://vehicle-rental-marketplace/output-data/location-vehicle"
        
        try:
            # Read input data with error handling
            locations = read_csv_with_retry(spark, input_paths["locations"], "locations")
            users = read_csv_with_retry(spark, input_paths["users"], "users")
            rental = read_csv_with_retry(spark, input_paths["rental"], "rental transactions")
            vehicles = read_csv_with_retry(spark, input_paths["vehicles"], "vehicles")
        except Exception as e:
            logger.error("Failed to read one or more input files. Aborting processing.")
            raise
        
        try:
            # Join rental transactions with vehicle data
            rental_vehicle_df = rental.join(vehicles, "vehicle_id", "left")
            write_parquet_with_checks(rental_vehicle_df, output_path, "rental-vehicledata")
            
            # Join with location data
            rental_vehicle_location_df = rental_vehicle_df.join(
                locations, rental_vehicle_df["pickup_location"] == locations["location_id"], "left"
            )
            write_parquet_with_checks(rental_vehicle_location_df, output_path, "rental-vehicle-location data")
        except Exception as e:
            logger.error("Failed in data joining operations")
            raise
        
        try:
            # Calculate revenue per location
            revenue_per_location = rental.groupBy("pickup_location").agg(
                sum("total_amount").alias("total_revenue")
            ).join(
                locations, rental["pickup_location"] == locations["location_id"], "left"
            ).select(
                col("location_id"),
                col("location_name"),
                col("total_revenue")
            )
            write_parquet_with_checks(revenue_per_location, output_path, "revenue per location")
        except Exception as e:
            logger.error("Failed in revenue per location calculation")
            raise
        
        try:
            # Compute total transactions per location
            transactions_per_location = rental.groupBy("pickup_location").agg(
                count("rental_id").alias("total_transactions")
            ).join(
                locations, rental["pickup_location"] == locations["location_id"], "left"
            ).select(
                col("location_id"),
                col("location_name"),
                col("total_transactions")
            )
            write_parquet_with_checks(transactions_per_location, output_path, "transactions per location")
        except Exception as e:
            logger.error("Failed in transactions per location calculation")
            raise
        
        try:
            # Compute average transaction amount per location
            avg_transaction_per_location = rental.groupBy("pickup_location").agg(
                avg("total_amount").alias("avg_transaction_amount")
            ).join(
                locations, rental["pickup_location"] == locations["location_id"], "left"
            ).select(
                col("location_id"),
                col("location_name"),
                col("avg_transaction_amount")
            )
            write_parquet_with_checks(avg_transaction_per_location, output_path, "average transaction per location")
        except Exception as e:
            logger.error("Failed in average transaction calculation")
            raise
        
        try:
            # Compute max and min transaction amount per location
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
            write_parquet_with_checks(max_min_transaction_per_location, output_path, "max/min transaction per location")
        except Exception as e:
            logger.error("Failed in max/min transaction calculation")
            raise
        
        try:
            # Compute unique vehicles used per location
            unique_vehicles_per_location = rental.groupBy("pickup_location").agg(
                countDistinct("vehicle_id").alias("unique_vehicles_used")
            ).join(
                locations, rental["pickup_location"] == locations["location_id"], "left"
            ).select(
                col("location_id"),
                col("location_name"),
                col("unique_vehicles_used")
            )
            write_parquet_with_checks(unique_vehicles_per_location, output_path, "unique vehicles per location")
        except Exception as e:
            logger.error("Failed in unique vehicles calculation")
            raise
        
        try:
            # Calculate rental duration metrics
            rental_duration_metrics_by_vehicle_type = rental_vehicle_df.groupBy("vehicle_type").agg(
                avg("rental_duration_hours").alias("avg_rental_duration"),
                max("rental_duration_hours").alias("max_rental_duration"),
                min("rental_duration_hours").alias("min_rental_duration"),
                sum("rental_duration_hours").alias("total_rental_duration")
            )
            write_parquet_with_checks(rental_duration_metrics_by_vehicle_type, output_path, "rental duration metrics")
        except Exception as e:
            logger.error("Failed in rental duration metrics calculation")
            raise
        
        logger.info("All processing completed successfully")
        
    except Exception as e:
        logger.error(f"Critical failure in main processing: {str(e)}")
        sys.exit(1)
    finally:
        try:
            spark.stop()
            logger.info("Spark session stopped successfully")
        except Exception as e:
            logger.error(f"Error while stopping Spark session: {str(e)}")

if __name__ == "__main__":
    main()