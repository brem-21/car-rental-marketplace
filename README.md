# Car Rental Marketplace Data Pipeline

## Project Overview
The project is based on a dataset from a car rental marketplace, a digital platform that connects vehicle owners with individuals seeking short-term rental options. This marketplace operates at scale, handling high volumes of user interactions, vehicle listings, rental transactions, and real-time location data. The company relies heavily on data to optimize pricing, enhance user experience, and ensure vehicle availability and operational efficiency.

## Data Pipeline
The pipeline aims to ingest data from an Amazon S3 bucket in raw format and process it using an EMR Cluster. The processing comes in two phases (Spark Jobs) written in parquet format:

1. **Vehicle and Location Performance Metrics**
   - Revenue per Location
   - Total transactions per location
   - Average, max, and min transaction amounts
   - Unique vehicles used at each location
   - Rental duration and revenue by vehicle type

2. **User and Transaction Analysis**
   - Total transactions per day
   - Revenue per day
   - User-specific spending and rental duration metrics
   - Maximum and minimum transaction amounts

## Schema Documentation

### Rental Transaction Table
| Column Name | Column Type | Description |
|-------------|-------------|-------------|
| Rental id | String | Unique identifier for each rental transaction |
| User id | String | ID of the user who made the rental |
| Vehicle id | String | ID of the vehicle rented |
| Rental start time | timestamp | When the rental began |
| Rental end time | timestamp | When the rental ended |
| Pickup location | Integer | Location ID where vehicle was picked up |
| Dropoff location | Integer | Location ID where vehicle was dropped off |
| Total amount | Double | Total cost charged for the rental |

### Location Table
| Column Name | Column Type | Description |
|-------------|-------------|-------------|
| Location id | Integer | Unique identifier for the location |
| Location name | String | Descriptive name of the location |
| Address | String | Street address |
| City | String | City where located |
| State | String | State where located |
| Zip code | Integer | ZIP/postal code |
| Latitude | Double | Latitude coordinate |
| Longitude | Double | Longitude coordinate |

### Users Table
| Column Name | Column Type | Description |
|-------------|-------------|-------------|
| User id | String | User identifier |
| First name | String | User's first name |
| Last name | String | User's last name |
| Email | String | User's email |
| Phone number | String | User phone number |
| Driver license number | String | License number |
| Driver license expiry | Date | License expiry date |
| Creation date | Date | User creation date |
| Is Active | integer | Status (1 = active, 0 = inactive) |

### Vehicles Table
| Column Name | Column Type | Description |
|-------------|-------------|-------------|
| Active | Integer | Status flag (1 = available) |
| Vehicle license number | String | License plate number |
| Registration name | String | Registration name |
| License type | String | Type of vehicle license |
| Expiration date | String | Registration expiry date |
| Permit license number | String | Permit/license number |
| Certification date | Date | Certification date |
| Vehicle year | Integer | Manufacture year |
| Base telephone number | String | Base phone number |
| Base address | String | Base physical address |
| Vehicle id | String | Unique vehicle ID |
| Last update timestamp | String | Last info update |
| Brand | String | Manufacturer brand |
| Vehicle type | String | Vehicle category |

## Orchestration Architecture
The pipeline follows this sequence:
1. Create EMR Cluster
2. Run Spark Jobs in Parallel
3. Trigger Glue Crawler
4. Query Athena (Multiple Queries)
5. Terminate EMR Cluster

### Key Components
- **EMR Cluster**: Version 6.12.0 with Spark, 1 master + 2 core nodes (m5.xlarge)
- **Spark Jobs**: Two parallel jobs for location-vehicle and user-transaction analysis
- **Glue Crawler**: Discovers and catalogs processed data schema
- **Athena Queries**: Analyzes processed data for business insights

## Monitoring and Troubleshooting
1. **Execution Monitoring**: Step Functions provide visual execution tracking
2. **Logs and Debugging**:
   - Review EMR logs in S3
   - Check Athena query results
   - Review CloudWatch logs
3. **Common Issues**:
   - Permission errors
   - Resource limits
   - S3 access problems
   - Glue crawler configuration

## Security Considerations
1. IAM Roles: Follow least privilege principle
2. S3 Buckets: Implement appropriate bucket policies
3. EMR Security: Consider encryption configurations
4. Networking: Use VPC and security groups

## Cost Optimization
1. **EMR Cluster**:
   - Terminate clusters after use
   - Consider Spot instances for core nodes
   - Evaluate instance types based on workload
