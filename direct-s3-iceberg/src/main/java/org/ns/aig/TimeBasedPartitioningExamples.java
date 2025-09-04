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
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Time-Based Partitioning Examples for AIG Events Table
 * Demonstrates various time-based partitioning strategies and query patterns
 */
public class TimeBasedPartitioningExamples {

    private static final String WAREHOUSE = "s3://ns-dpl-ice-poc/aig/";
    private static final String DATABASE = "dpl_events_ice";
    private static final String TABLE_NAME = "events";
    private static final String S3A_ENDPOINT = "s3.us-west-2.amazonaws.com";

    public static void main(String[] args) throws Exception {
        System.out.println("=== Time-Based Partitioning Examples ===\n");

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

            // Demonstrate different time-based partitioning concepts
            demonstratePartitioningConcepts();

            // Show query examples with partition pruning
            demonstrateTimeBasedQueries(table);

            // Show partition evolution examples
            demonstratePartitionEvolution(catalog, table);

            // Generate time-series data examples
            generateTimeSeriesExamples(table);

        } catch (Exception e) {
            System.err.println("❌ Time-based partitioning examples FAILED!");
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    private static Configuration setupHadoopConfiguration() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.endpoint", S3A_ENDPOINT);
        hadoopConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        return hadoopConf;
    }

    private static GlueCatalog setupGlueCatalog(Configuration hadoopConf) {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE);
        catalogProps.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
        catalogProps.put("glue.skip-name-validation", "true");

        GlueCatalog catalog = new GlueCatalog();
        catalog.setConf(hadoopConf);
        catalog.initialize("glue", catalogProps);
        return catalog;
    }

    private static void demonstratePartitioningConcepts() {
        System.out.println("\n📚 TIME-BASED PARTITIONING CONCEPTS");
        System.out.println("====================================");

        System.out.println("1. TRUNCATION-BASED PARTITIONING:");
        System.out.println("   - truncate(timestamp, 86400000) = Daily partitions");
        System.out.println("   - truncate(timestamp, 3600000) = Hourly partitions");
        System.out.println("   - truncate(timestamp, 2592000000) = Monthly partitions (30 days)");

        System.out.println("\n2. BENEFITS OF TIME-BASED PARTITIONING:");
        System.out.println("   ✓ Partition pruning for time-range queries");
        System.out.println("   ✓ Efficient data organization in storage");
        System.out.println("   ✓ Improved query performance");
        System.out.println("   ✓ Better data lifecycle management");
        System.out.println("   ✓ Parallel processing capabilities");

        System.out.println("\n3. PARTITION GRANULARITY TRADE-OFFS:");
        System.out.println("   Fine-grained (hourly/minute):");
        System.out.println("     + Better query selectivity");
        System.out.println("     + Smaller scan ranges");
        System.out.println("     - More metadata overhead");
        System.out.println("     - Potential small file problem");

        System.out.println("\n   Coarse-grained (daily/weekly):");
        System.out.println("     + Less metadata overhead");
        System.out.println("     + Larger, more efficient files");
        System.out.println("     - Broader scan ranges");
        System.out.println("     - Less query selectivity");

        // Show examples of different partition calculations
        long baseTime = System.currentTimeMillis();
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(baseTime), ZoneOffset.UTC);

        System.out.println("\n4. PARTITION VALUE CALCULATIONS:");
        System.out.println("   Current timestamp: " + baseTime + " (" +
            dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + ")");

        // Daily partition
        long dailyPartition = (baseTime / 86400000L) * 86400000L;
        System.out.println("   Daily partition: " + dailyPartition + " (" +
            LocalDateTime.ofInstant(Instant.ofEpochMilli(dailyPartition), ZoneOffset.UTC).toLocalDate() + ")");

        // Hourly partition
        long hourlyPartition = (baseTime / 3600000L) * 3600000L;
        System.out.println("   Hourly partition: " + hourlyPartition + " (" +
            LocalDateTime.ofInstant(Instant.ofEpochMilli(hourlyPartition), ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + ")");

        // Weekly partition (7 days)
        long weeklyPartition = (baseTime / 604800000L) * 604800000L;
        System.out.println("   Weekly partition: " + weeklyPartition + " (" +
            LocalDateTime.ofInstant(Instant.ofEpochMilli(weeklyPartition), ZoneOffset.UTC).toLocalDate() + ")");
    }

    private static void demonstrateTimeBasedQueries(Table table) {
        System.out.println("\n🔍 TIME-BASED QUERY EXAMPLES");
        System.out.println("============================");

        long now = System.currentTimeMillis();
        long oneDayAgo = now - 86400000L;
        long oneHourAgo = now - 3600000L;

        System.out.println("Query patterns that benefit from time-based partitioning:\n");

        System.out.println("1. RECENT DATA QUERIES (Last 24 hours):");
        System.out.println("   Filter: timestamp >= " + oneDayAgo);
        System.out.println("   Iceberg expression: Expressions.greaterThanOrEqual(\"timestamp\", " + oneDayAgo + "L)");
        System.out.println("   Benefit: Only scans today's partition");

        System.out.println("\n2. SPECIFIC TIME RANGE QUERIES:");
        System.out.println("   Filter: timestamp BETWEEN " + oneHourAgo + " AND " + now);
        System.out.println("   Iceberg expression: Expressions.and(");
        System.out.println("     Expressions.greaterThanOrEqual(\"timestamp\", " + oneHourAgo + "L),");
        System.out.println("     Expressions.lessThanOrEqual(\"timestamp\", " + now + "L))");
        System.out.println("   Benefit: Only scans relevant hourly partitions");

        System.out.println("\n3. DAILY AGGREGATION QUERIES:");
        long startOfDay = (now / 86400000L) * 86400000L;
        System.out.println("   Filter: timestamp >= " + startOfDay + " (start of today)");
        System.out.println("   Benefit: Scans only today's partition for daily metrics");

        System.out.println("\n4. MULTI-TENANT TIME QUERIES:");
        System.out.println("   Filter: tenant_id = 1001 AND timestamp >= " + oneDayAgo);
        System.out.println("   Iceberg expression: Expressions.and(");
        System.out.println("     Expressions.equal(\"tenant_id\", 1001),");
        System.out.println("     Expressions.greaterThanOrEqual(\"timestamp\", " + oneDayAgo + "L))");
        System.out.println("   Benefit: Partition pruning on both tenant and time dimensions");

        // Demonstrate actual query execution if table has data
        try {
            System.out.println("\n5. ACTUAL QUERY EXECUTION EXAMPLE:");
            org.apache.iceberg.TableScan scan = table.newScan()
                .filter(Expressions.greaterThanOrEqual("timestamp", oneHourAgo));

            System.out.println("   Scanning records from last hour...");
            int count = 0;
            try (CloseableIterable<org.apache.iceberg.CombinedScanTask> tasks = scan.planTasks()) {
                for (org.apache.iceberg.CombinedScanTask task : tasks) {
                    count += task.files().size();
                }
            }
            System.out.println("   Files to scan: " + count);

        } catch (Exception e) {
            System.out.println("   Query execution example skipped: " + e.getMessage());
        }
    }

    private static void demonstratePartitionEvolution(GlueCatalog catalog, Table table) {
        System.out.println("\n🔄 PARTITION EVOLUTION EXAMPLES");
        System.out.println("===============================");

        System.out.println("Iceberg supports partition evolution - changing partition strategy over time:");
        System.out.println("\nCurrent partition spec:");
        PartitionSpec currentSpec = table.spec();
        for (PartitionField field : currentSpec.fields()) {
            System.out.println("  - " + field.name() + ": " + field.transform());
        }

        System.out.println("\nExample evolution scenarios:");

        System.out.println("\n1. GRANULARITY CHANGE:");
        System.out.println("   Before: Daily partitions only");
        System.out.println("   After: Add hourly sub-partitions for recent data");
        System.out.println("   SQL: ALTER TABLE events ADD PARTITION FIELD hour(timestamp)");

        System.out.println("\n2. NEW DIMENSION:");
        System.out.println("   Before: Time-based partitioning only");
        System.out.println("   After: Add geographic partitioning");
        System.out.println("   SQL: ALTER TABLE events ADD PARTITION FIELD home_pop");

        System.out.println("\n3. PARTITION REMOVAL:");
        System.out.println("   Remove old partition fields that are no longer useful");
        System.out.println("   SQL: ALTER TABLE events DROP PARTITION FIELD old_field");

        System.out.println("\nPartition evolution benefits:");
        System.out.println("  ✓ No data rewrite required");
        System.out.println("  ✓ Backwards compatibility");
        System.out.println("  ✓ Gradual migration");
        System.out.println("  ✓ Query optimization over time");
    }

    private static void generateTimeSeriesExamples(Table table) {
        System.out.println("\n📊 TIME-SERIES DATA PATTERNS");
        System.out.println("=============================");

        System.out.println("Common time-series patterns for AIG Events:");

        System.out.println("\n1. HIGH-FREQUENCY EVENTS (Real-time AI requests):");
        System.out.println("   - Partition strategy: Hourly or sub-hourly");
        System.out.println("   - Data retention: 30-90 days");
        System.out.println("   - Query pattern: Recent time windows");

        System.out.println("\n2. BATCH PROCESSING EVENTS (Daily aggregations):");
        System.out.println("   - Partition strategy: Daily");
        System.out.println("   - Data retention: 1-2 years");
        System.out.println("   - Query pattern: Historical analysis");

        System.out.println("\n3. COMPLIANCE EVENTS (Audit logs):");
        System.out.println("   - Partition strategy: Daily with tenant isolation");
        System.out.println("   - Data retention: 7+ years");
        System.out.println("   - Query pattern: Compliance reporting");

        // Generate example time-series data patterns
        System.out.println("\n4. SAMPLE TIME-SERIES DATA DISTRIBUTION:");
        long now = System.currentTimeMillis();
        Map<String, Integer> hourlyDistribution = new HashMap<>();

        // Simulate 24 hours of data distribution
        for (int hour = 0; hour < 24; hour++) {
            long hourTimestamp = now - (hour * 3600000L);
            long hourlyPartition = (hourTimestamp / 3600000L) * 3600000L;
            String hourLabel = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(hourlyPartition), ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00"));

            // Simulate varying load throughout the day
            int eventCount = ThreadLocalRandom.current().nextInt(50, 500);
            if (hour >= 8 && hour <= 18) { // Business hours
                eventCount = ThreadLocalRandom.current().nextInt(200, 1000);
            }

            hourlyDistribution.put(hourLabel, eventCount);
        }

        System.out.println("   Recent 24-hour distribution (simulated):");
        hourlyDistribution.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .limit(6) // Show first 6 hours
            .forEach(entry -> System.out.println("     " + entry.getKey() + ": " + entry.getValue() + " events"));
        System.out.println("     ... (and 18 more hours)");

        System.out.println("\n5. PARTITION PRUNING EFFICIENCY:");
        System.out.println("   Query: Last 4 hours of events");
        System.out.println("   Without partitioning: Scan all data files");
        System.out.println("   With hourly partitioning: Scan only 4 partition directories");
        System.out.println("   Efficiency gain: 83-95% reduction in files scanned (typical)");

        System.out.println("\n6. STORAGE OPTIMIZATION:");
        System.out.println("   - Old partitions can be compressed differently");
        System.out.println("   - Lifecycle policies for automated archival");
        System.out.println("   - Different storage classes (Standard → IA → Glacier)");
        System.out.println("   - Partition-level metadata for quick queries");
    }
}
