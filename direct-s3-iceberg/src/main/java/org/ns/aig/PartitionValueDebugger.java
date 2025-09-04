package org.ns.aig;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Partition Value Debugger for AIG Events Table
 * Helps debug and understand how records are partitioned in the Iceberg table
 */
public class PartitionValueDebugger {

    private static final String WAREHOUSE = "s3://ns-dpl-ice-poc/aig/";
    private static final String DATABASE = "dpl_events_ice";
    private static final String TABLE_NAME = "events";
    private static final String S3A_ENDPOINT = "s3.us-west-2.amazonaws.com";

    public static void main(String[] args) throws Exception {
        System.out.println("=== AIG Events Partition Value Debugger ===\n");

        try {
            // Setup Hadoop and Glue configuration
            Configuration hadoopConf = setupHadoopConfiguration();
            GlueCatalog catalog = setupGlueCatalog(hadoopConf);

            // Load the existing table
            TableIdentifier tableId = TableIdentifier.of(DATABASE, TABLE_NAME);
            if (!catalog.tableExists(tableId)) {
                System.err.println("❌ Table does not exist: " + tableId);
                System.err.println("Please run AIGEventsTableCreator first to create the table.");
                return;
            }

            Table table = catalog.loadTable(tableId);
            System.out.println("✓ Loaded table: " + tableId);

            // Debug partition information
            debugPartitionSpec(table);
            debugPartitionValues(table);
            debugDataFiles(table);

            // Show examples of partition value calculations
            showPartitionValueExamples();

        } catch (Exception e) {
            System.err.println("❌ Partition debugging FAILED!");
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    private static Configuration setupHadoopConfiguration() {
        System.out.println("Setting up Hadoop configuration...");

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.endpoint", S3A_ENDPOINT);
        hadoopConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

        System.out.println("✓ Hadoop configuration ready");
        return hadoopConf;
    }

    private static GlueCatalog setupGlueCatalog(Configuration hadoopConf) {
        System.out.println("Setting up Glue catalog...");

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE);
        catalogProps.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
        catalogProps.put("glue.skip-name-validation", "true");

        GlueCatalog catalog = new GlueCatalog();
        catalog.setConf(hadoopConf);
        catalog.initialize("glue", catalogProps);

        System.out.println("✓ Glue catalog initialized");
        return catalog;
    }

    private static void debugPartitionSpec(Table table) {
        System.out.println("\n📊 PARTITION SPECIFICATION DEBUG");
        System.out.println("================================");

        PartitionSpec spec = table.spec();
        System.out.println("Partition spec ID: " + spec.specId());
        System.out.println("Number of partition fields: " + spec.fields().size());

        System.out.println("\nPartition fields:");
        for (PartitionField field : spec.fields()) {
            System.out.println("  Field ID: " + field.fieldId());
            System.out.println("  Name: " + field.name());
            System.out.println("  Source column: " + field.sourceId());
            System.out.println("  Transform: " + field.transform());
            System.out.println("  ---");
        }
    }

    private static void debugPartitionValues(Table table) throws Exception {
        System.out.println("\n🔍 PARTITION VALUES DEBUG");
        System.out.println("=========================");

        // Read some sample records to see their partition values
        try {
            // Use Iceberg's data reader to scan records - use the public API correctly
            try (CloseableIterable<org.apache.iceberg.data.Record> records = org.apache.iceberg.data.IcebergGenerics.read(table).build()) {
                int count = 0;
                for (org.apache.iceberg.data.Record record : records) {
                    if (count >= 5) break; // Show first 5 records

                    System.out.println("\nRecord " + (count + 1) + ":");

                    // Extract partition-related fields - cast to GenericRecord for field access
                    GenericRecord genericRecord = (GenericRecord) record;
                    Object tenantId = genericRecord.getField("tenant_id");
                    Object timestamp = genericRecord.getField("timestamp");
                    Object serviceId = genericRecord.getField("service_id");

                    System.out.println("  tenant_id: " + tenantId);
                    System.out.println("  service_id: " + serviceId);
                    System.out.println("  timestamp: " + timestamp);

                    if (timestamp instanceof Long) {
                        long ts = (Long) timestamp;
                        LocalDateTime dateTime = LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(ts), ZoneOffset.UTC);
                        System.out.println("  timestamp (readable): " +
                            dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

                        // Calculate partition values manually
                        long dailyPartition = (ts / 86400000L) * 86400000L; // Daily truncation
                        long hourlyPartition = (ts / 3600000L) * 3600000L;  // Hourly truncation

                        System.out.println("  Calculated daily partition: " + dailyPartition +
                            " (" + LocalDateTime.ofInstant(Instant.ofEpochMilli(dailyPartition), ZoneOffset.UTC) + ")");
                        System.out.println("  Calculated hourly partition: " + hourlyPartition +
                            " (" + LocalDateTime.ofInstant(Instant.ofEpochMilli(hourlyPartition), ZoneOffset.UTC) + ")");
                    }
                    count++;
                }

                if (count == 0) {
                    System.out.println("No records found in the table. Please run AIGEventsTableCreator first.");
                }
            }
        } catch (Exception e) {
            System.out.println("Error reading records: " + e.getMessage());
            System.out.println("This may be expected if the table is empty or inaccessible.");
        }
    }

    private static void debugDataFiles(Table table) {
        System.out.println("\n📁 DATA FILES DEBUG");
        System.out.println("===================");

        // Get table snapshots
        for (Snapshot snapshot : table.snapshots()) {
            System.out.println("Snapshot ID: " + snapshot.snapshotId());
            System.out.println("Timestamp: " + Instant.ofEpochMilli(snapshot.timestampMillis()));
            System.out.println("Operation: " + snapshot.operation());

            if (snapshot.addedDataFiles(table.io()) != null) {
                System.out.println("Added data files:");
                try {
                    int fileCount = 0;
                    for (DataFile dataFile : snapshot.addedDataFiles(table.io())) {
                        if (fileCount >= 3) { // Show first 3 files
                            System.out.println("  ... and more files");
                            break;
                        }
                        System.out.println("  File: " + dataFile.path());
                        System.out.println("    Partition: " + dataFile.partition());
                        System.out.println("    Record count: " + dataFile.recordCount());
                        System.out.println("    File size: " + dataFile.fileSizeInBytes() + " bytes");
                        fileCount++;
                    }
                } catch (Exception e) {
                    System.out.println("  Error reading data files: " + e.getMessage());
                }
            }
            System.out.println("  ---");
            break; // Show only the latest snapshot
        }
    }

    private static void showPartitionValueExamples() {
        System.out.println("\n🧮 PARTITION VALUE CALCULATION EXAMPLES");
        System.out.println("=======================================");

        // Show examples of how partition values are calculated
        long now = System.currentTimeMillis();
        long[] sampleTimestamps = {
            now,
            now - 3600000L,  // 1 hour ago
            now - 86400000L, // 1 day ago
            now - 172800000L // 2 days ago
        };

        for (long timestamp : sampleTimestamps) {
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);

            // Calculate partition values as Iceberg would
            long dailyPartition = (timestamp / 86400000L) * 86400000L;
            long hourlyPartition = (timestamp / 3600000L) * 3600000L;

            System.out.println("\nTimestamp: " + timestamp);
            System.out.println("  Readable: " + dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            System.out.println("  Daily partition: " + dailyPartition +
                " (" + LocalDateTime.ofInstant(Instant.ofEpochMilli(dailyPartition), ZoneOffset.UTC).toLocalDate() + ")");
            System.out.println("  Hourly partition (ts_hour): " + hourlyPartition +
                " (" + LocalDateTime.ofInstant(Instant.ofEpochMilli(hourlyPartition), ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + ")");
        }

        System.out.println("\n💡 PARTITION STRATEGY EXPLANATION");
        System.out.println("=================================");
        System.out.println("The AIG Events table uses the following partitioning strategy:");
        System.out.println("1. tenant_id (identity) - Direct partitioning by tenant for multi-tenancy");
        System.out.println("2. timestamp truncated to day (86400000ms) - Daily partitions for time-based queries");
        System.out.println("3. timestamp truncated to hour (3600000ms) as 'ts_hour' - Hourly sub-partitions for granular access");
        System.out.println("4. service_id (identity) - Direct partitioning by service for performance isolation");
        System.out.println("\nThis creates a hierarchical partition structure that enables:");
        System.out.println("- Efficient tenant isolation");
        System.out.println("- Fast time-range queries");
        System.out.println("- Service-specific data access");
        System.out.println("- Optimal file organization in S3");
    }
}
