# Java File Summaries

This project contains several Java files for working with Apache Iceberg tables and Parquet files on AWS S3. Below is a summary of each main Java file:

- **BulkParquetToIcebergAtomicMultipart.java**
  - Handles bulk loading of multiple local Parquet files into an Apache Iceberg table on S3 using AWS Glue as the catalog. Supports batching and multi-threaded processing for efficient ingestion.

- **HiddenPartitionLoader.java**
  - Loads Parquet files into an Iceberg table, focusing on handling "hidden" or dynamic partition values (e.g., extracting partition values from data rather than file paths). Uses AWS Glue and S3.

- **LocalReadParquetToS3Iceberg.java**
  - Demonstrates reading a single local Parquet file, converting its schema, and writing it to an Iceberg table on S3. Includes logic for handling partition columns and schema conversion.

- **Main.java**
  - Provides a minimal example of reading a Parquet file (from S3), converting its schema to Iceberg, defining a partition spec, and initializing the Glue catalog. Serves as a reference for basic schema and catalog operations.

---

For more details, see the source code in `src/main/java/org/ns/`.

