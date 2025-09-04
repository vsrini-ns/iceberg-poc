package org.ns.aig;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * AIG Events Table Creator and Data Generator
 * Creates Iceberg table with AIG Events schema and generates test data
 */
public class AIGEventsTableCreator {

    // Configuration constants
    private static final String WAREHOUSE = "s3://ns-dpl-ice-poc/aig/";
    private static final String DATABASE = "dpl_events_ice";
    private static final String TABLE_NAME = "events";
    private static final String S3A_ENDPOINT = "s3.us-west-2.amazonaws.com";

    // Test data generation parameters
    private static final int NUM_TEST_RECORDS = 100;
    private static final String[] AI_PROVIDERS = {"openai", "anthropic", "cohere", "google", "aws-bedrock"};
    private static final String[] SERVICE_IDS = {"ai-gateway-v1", "ai-gateway-v2", "ai-proxy", "ml-service"};
    private static final String[] HOME_POPS = {"us-west-2", "us-east-1", "eu-west-1", "ap-southeast-1"};
    private static final String[] REQUEST_TYPES = {"inference", "training", "embedding", "completion"};
    private static final String[] MODELS = {"gpt-4", "claude-3", "llama-2", "gemini-pro", "titan-xl"};

    public static void main(String[] args) throws Exception {
        System.out.println("=== AIG Events Table Creator ===\n");

        try {
            // Setup Hadoop and Glue configuration
            Configuration hadoopConf = setupHadoopConfiguration();
            GlueCatalog catalog = setupGlueCatalog(hadoopConf);

            // Create the events schema
            Schema eventsSchema = AIGEventsSchemaValidator.createEventsSchema();
            System.out.println("✓ AIG Events schema loaded with " + eventsSchema.columns().size() + " fields");

            // Create table with partitioning
            Table table = createOrUpdateTable(catalog, eventsSchema);
            System.out.println("✓ AIG Events table created/updated successfully");

            // Generate and write test data atomically
            generateAndWriteTestData(table, eventsSchema);

            System.out.println("\n🎉 AIG Events table creation and data generation completed successfully!");
            System.out.println("✅ Table: " + DATABASE + "." + TABLE_NAME);
            System.out.println("✅ Records written: " + NUM_TEST_RECORDS);
            System.out.println("✅ Format: Parquet with ZSTD compression");
            System.out.println("✅ Operation: Atomic commit");

        } catch (Exception e) {
            System.err.println("❌ AIG Events table creation FAILED!");
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

    private static Table createOrUpdateTable(GlueCatalog catalog, Schema schema) {
        System.out.println("Creating/updating AIG Events table...");

        TableIdentifier tableId = TableIdentifier.of(DATABASE, TABLE_NAME);
        Namespace namespace = Namespace.of(DATABASE);

        // Create namespace if it doesn't exist
        if (!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace);
            System.out.println("✓ Created namespace: " + DATABASE);
        }

        Table table;
        if (catalog.tableExists(tableId)) {
            table = catalog.loadTable(tableId);
            System.out.println("✓ Loaded existing table: " + tableId);
        } else {
            // Create partition spec with time-based partitioning using long timestamp
            PartitionSpec spec = PartitionSpec.builderFor(schema)
                    .identity("tenant_id")                    // Partition by tenant for multi-tenancy
                    .truncate("timestamp", 86400000)          // Daily partitions (24*60*60*1000 ms)
                    .truncate("timestamp", 3600000, "ts_hour") // Hourly sub-partitions (60*60*1000 ms)
                    .identity("service_id")                   // Partition by service for performance
                    .build();

            table = catalog.createTable(tableId, schema, spec);
            System.out.println("✓ Created new table with time-based partitions: " + tableId);
            System.out.println("  - Daily partitions based on timestamp truncation");
            System.out.println("  - Hourly sub-partitions for granular access");
            System.out.println("  - Tenant and service partitions for isolation");
        }

        // Set table properties for optimal performance
        table.updateProperties()
                .set(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name())
                .set("write.parquet.compression-codec", "zstd")
                .set("write.target-file-size-bytes", "134217728") // 128MB
                .set("write.parquet.page-size-bytes", "1048576")   // 1MB
                .set("write.parquet.dict-size-bytes", "2097152")   // 2MB
                .commit();

        System.out.println("✓ Table properties configured for ZSTD compression");
        return table;
    }

    private static void generateAndWriteTestData(Table table, Schema schema) throws Exception {
        System.out.println("Generating and writing " + NUM_TEST_RECORDS + " test records...");

        // Generate test records
        List<GenericRecord> records = generateTestRecords(schema);
        System.out.println("✓ Generated " + records.size() + " test records");

        // Write records using Iceberg's data writer for proper partitioning
        writeRecordsWithPartitioning(table, records);
        System.out.println("✓ Records written atomically to S3 in Parquet/ZSTD format");
    }

    private static List<GenericRecord> generateTestRecords(Schema schema) {
        List<GenericRecord> records = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long baseTime = Instant.now().minus(7, ChronoUnit.DAYS).toEpochMilli();

        for (int i = 0; i < NUM_TEST_RECORDS; i++) {
            GenericRecord record = GenericRecord.create(schema);

            // Required fields
            record.setField("tenant_id", 1000 + (i % 10)); // 10 different tenants
            record.setField("home_pop", HOME_POPS[i % HOME_POPS.length]);
            record.setField("service_id", SERVICE_IDS[i % SERVICE_IDS.length]);
            record.setField("timestamp", baseTime + (i * 3600000L)); // Hourly intervals

            // Optional basic fields
            record.setField("transaction_id", 100000 + i);
            record.setField("response_id", "resp-" + UUID.randomUUID().toString().substring(0, 8));
            record.setField("version", "1." + (i % 5) + ".0");
            record.setField("type", REQUEST_TYPES[i % REQUEST_TYPES.length]);
            record.setField("gateway_id", "gw-" + (i % 3));
            record.setField("ai_provider_id", AI_PROVIDERS[i % AI_PROVIDERS.length]);
            record.setField("cs_model", MODELS[i % MODELS.length]);
            record.setField("rs_model", MODELS[i % MODELS.length]);
            record.setField("rs_status", random.nextBoolean() ? 200 : (random.nextBoolean() ? 400 : 500));
            record.setField("rs_response_time", random.nextInt(50, 5000));
            record.setField("usage_total", random.nextInt(10, 1000));
            record.setField("usage_input", random.nextInt(5, 500));
            record.setField("usage_output", random.nextInt(5, 500));

            // Complex policy data (20% of records)
            if (i % 5 == 0) {
                record.setField("policy", generatePolicyData(schema, i));
            }

            // File metadata (30% of records)
            if (i % 3 == 0) {
                record.setField("cs_files", generateFileData(schema, "cs_files", i, "input"));
            }
            if (i % 4 == 0) {
                record.setField("rs_files", generateFileData(schema, "rs_files", i, "output"));
            }

            // Rate limiting data (50% of records)
            if (i % 2 == 0) {
                record.setField("ratelimit", generateRateLimitData());
            }

            // Additional optional fields
            record.setField("action", random.nextBoolean() ? "allow" : "throttle");
            record.setField("activity", "ai_request_" + (i % 3));
            record.setField("conversation_id", "conv-" + UUID.randomUUID().toString().substring(0, 12));

            if (random.nextDouble() < 0.1) { // 10% incident rate
                record.setField("incident_id", 9000 + random.nextInt(100));
            }

            records.add(record);
        }

        return records;
    }

    private static List<GenericRecord> generatePolicyData(Schema schema, int recordIndex) {
        List<GenericRecord> policies = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int numPolicies = random.nextInt(1, 4); // 1-3 policies per record
        for (int j = 0; j < numPolicies; j++) {
            GenericRecord policy = GenericRecord.create(
                schema.findField("policy").type().asListType().elementType().asStructType()
            );

            policy.setField("type", j == 0 ? "rate_limit" : (j == 1 ? "auth" : "content_filter"));
            policy.setField("name", "policy_" + recordIndex + "_" + j);
            policy.setField("action", random.nextBoolean() ? "allow" : "deny");
            policy.setField("object_id", 2000 + recordIndex + j);

            // Generate profile list
            List<String> profiles = new ArrayList<>();
            String[] profileTypes = {"basic", "premium", "enterprise", "developer"};
            int numProfiles = random.nextInt(1, 3);
            for (int k = 0; k < numProfiles; k++) {
                profiles.add(profileTypes[k % profileTypes.length]);
            }
            policy.setField("profile", profiles);

            policies.add(policy);
        }

        return policies;
    }

    private static List<GenericRecord> generateFileData(Schema schema, String fieldName, int recordIndex, String purpose) {
        List<GenericRecord> files = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int numFiles = random.nextInt(1, 3); // 1-2 files per record
        for (int j = 0; j < numFiles; j++) {
            GenericRecord file = GenericRecord.create(
                schema.findField(fieldName).type().asListType().elementType().asStructType()
            );

            file.setField("object_id", 3000 + recordIndex + j);
            file.setField("purpose", purpose);
            file.setField("type", purpose.equals("input") ? "prompt" : "response");
            file.setField("mime_type", random.nextBoolean() ? "text/plain" : "application/json");
            file.setField("filename", purpose + "_" + recordIndex + "_" + j + ".txt");
            file.setField("bytes", (long) random.nextInt(100, 10000));

            long now = System.currentTimeMillis();
            file.setField("create_at", now - random.nextInt(3600000)); // Created within last hour
            file.setField("update_at", now);

            files.add(file);
        }

        return files;
    }

    private static List<String> generateRateLimitData() {
        List<String> rateLimits = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        String[] limitTypes = {"token_bucket", "sliding_window", "fixed_window", "adaptive_limit"};
        int numLimits = random.nextInt(1, 3);

        for (int i = 0; i < numLimits; i++) {
            rateLimits.add(limitTypes[i % limitTypes.length]);
        }

        return rateLimits;
    }

    private static void writeRecordsWithPartitioning(Table table, List<GenericRecord> records) throws Exception {
        System.out.println("Writing records with proper partitioning...");

        // Use Iceberg's data append API for simpler partitioned writes
        AppendFiles append = table.newAppend();

        // Use data writer factory to create files
        org.apache.iceberg.data.GenericAppenderFactory factory =
            new org.apache.iceberg.data.GenericAppenderFactory(table.schema(), table.spec());
        factory.setAll(table.properties());

        // Create data files by writing records and collecting them
        List<DataFile> dataFiles = new ArrayList<>();

        // Group records by partition to create separate files
        Map<String, List<GenericRecord>> partitionedRecords = new HashMap<>();

        for (GenericRecord record : records) {
            // Create a simple partition key based on tenant_id and service_id
            String partitionKey = record.getField("tenant_id") + "_" + record.getField("service_id");
            partitionedRecords.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(record);
        }

        // Write each partition group to separate files
        for (Map.Entry<String, List<GenericRecord>> entry : partitionedRecords.entrySet()) {
            String partitionKey = entry.getKey();
            List<GenericRecord> partitionRecords = entry.getValue();

            // Create a data writer for this partition - compute partition values properly
            String dataLocation = table.locationProvider().newDataLocation(partitionKey);
            org.apache.iceberg.io.OutputFile outputFile = table.io().newOutputFile(dataLocation);
            org.apache.iceberg.encryption.EncryptedOutputFile encryptedOutputFile = table.encryption().encrypt(outputFile);

            // Get the first record to compute partition values
            GenericRecord firstRecord = partitionRecords.get(0);

            // Create partition data structure using Iceberg's partition spec
            org.apache.iceberg.data.GenericRecord partitionRecord = org.apache.iceberg.data.GenericRecord.create(table.spec().partitionType());

            // Compute partition values according to the partition spec
            PartitionSpec spec = table.spec();
            for (int i = 0; i < spec.fields().size(); i++) {
                PartitionField field = spec.fields().get(i);
                String sourceName = table.schema().findColumnName(field.sourceId());
                Object sourceValue = firstRecord.getField(sourceName);

                // Get the partition field name
                String partitionFieldName = field.name();

                if (field.transform().isIdentity()) {
                    // Identity transform - use the value as-is
                    partitionRecord.setField(partitionFieldName, sourceValue);
                } else {
                    // Transform the value (e.g., truncate for timestamp)
                    if (sourceValue instanceof Long && field.transform().toString().startsWith("truncate")) {
                        Long longValue = (Long) sourceValue;
                        // Apply the transform
                        org.apache.iceberg.transforms.Transform<Long, Long> longTransform =
                            (org.apache.iceberg.transforms.Transform<Long, Long>) field.transform();
                        Long transformedValue = longTransform.apply(longValue);
                        partitionRecord.setField(partitionFieldName, transformedValue);
                    } else {
                        partitionRecord.setField(partitionFieldName, sourceValue);
                    }
                }
            }

            org.apache.iceberg.io.DataWriter<org.apache.iceberg.data.Record> dataWriter = null;
            try {
                dataWriter = factory.newDataWriter(encryptedOutputFile, FileFormat.PARQUET, partitionRecord);

                // Write records for this partition
                for (GenericRecord partitionRec : partitionRecords) {
                    dataWriter.write(partitionRec);
                }

                // Close and get the data file
                dataWriter.close();
                org.apache.iceberg.io.DataWriteResult result = dataWriter.result();

                // Add all data files from this writer
                dataFiles.addAll(result.dataFiles());

            } finally {
                if (dataWriter != null) {
                    try {
                        dataWriter.close();
                    } catch (Exception e) {
                        // Already closed
                    }
                }
            }
        }

        System.out.println("✓ Written " + records.size() + " records with computed partition values");
        System.out.println("✓ Data files created with partition information:");
        System.out.println("  - Records written: " + records.size());
        System.out.println("  - Data files: " + dataFiles.size());

        // Add all data files to the append operation
        for (DataFile dataFile : dataFiles) {
            append.appendFile(dataFile);
        }

        // Atomic commit - makes all data visible at once
        append.commit();

        System.out.println("✓ Atomic commit completed - all data visible with proper partition structure");
    }
}
