package org.ns.aig;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * AIG Events Table Creator and Data Generator
 * Creates Iceberg table with AIG Events schema and generates test data
 */
public class AIGEventsTableCreator {

    private static final Logger LOGGER = Logger.getLogger(AIGEventsTableCreator.class.getName());

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
            LOGGER.log(Level.SEVERE, "AIG Events table creation failed", e);
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
            // Create partition spec with exact hierarchical time-based partitioning
            // Format: type=<type>/tenant=<tenant>/year=<yyyy>/month=<mm>/day=<dd>/hour=<hh>/<UUID>.parquet
            PartitionSpec spec = PartitionSpec.builderFor(schema)
                    .identity("type")                          // type=<type>
                    .identity("tenant")                        // tenant=<tenant>
                    .identity("year")                          // year=<yyyy>
                    .identity("month")                         // month=<mm>
                    .identity("day")                           // day=<dd>
                    .identity("hour")                          // hour=<hh>
                    .build();

            table = catalog.createTable(tableId, schema, spec);
            System.out.println("✓ Created new table with hierarchical partitions: " + tableId);
            System.out.println("  - Type partitions: type=<type>");
            System.out.println("  - Tenant partitions: tenant=<tenant>");
            System.out.println("  - Year partitions: year=<yyyy>");
            System.out.println("  - Month partitions: month=<mm>");
            System.out.println("  - Day partitions: day=<dd>");
            System.out.println("  - Hour partitions: hour=<hh>");
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
        // Use current time for better hierarchical partitioning demonstration
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < NUM_TEST_RECORDS; i++) {
            GenericRecord record = GenericRecord.create(schema);

            // Required fields
            record.setField("tenant_id", 1000 + (i % 10)); // 10 different tenants
            record.setField("home_pop", HOME_POPS[i % HOME_POPS.length]);
            record.setField("service_id", SERVICE_IDS[i % SERVICE_IDS.length]);

            // Set timestamp and derive partition fields for exact path structure
            record.setField("timestamp", timestamp);

            // Add derived fields for exact partition structure using SHORT field names
            java.time.Instant instant = java.time.Instant.ofEpochMilli(timestamp);
            java.time.ZonedDateTime zdt = instant.atZone(java.time.ZoneOffset.UTC);

            // Populate the SHORT partition fields that will be used in S3 paths
            record.setField("tenant", 1000 + (i % 10));        // tenant=<tenant>
            record.setField("year", zdt.getYear());            // year=<yyyy>
            record.setField("month", zdt.getMonthValue());      // month=<mm>
            record.setField("day", zdt.getDayOfMonth());        // day=<dd>
            record.setField("hour", zdt.getHour());             // hour=<hh>

            // Optional basic fields
            record.setField("transaction_id", 100000 + i);
            record.setField("response_id", "resp-" + UUID.randomUUID().toString().substring(0, 8));
            record.setField("version", "1." + (i % 5) + ".0");
            record.setField("type", "aig"); // Set type to "aig" for the hierarchical path structure
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

    @SuppressWarnings("unchecked")
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
            // Create partition key using all partition fields: type, tenant_id, and timestamp-based partitions
            String type = (String) record.getField("type");
            Integer tenantId = (Integer) record.getField("tenant_id");
            Long timestamp = (Long) record.getField("timestamp");

            // Create a comprehensive partition key
            String partitionKey = type + "_" + tenantId + "_" + timestamp;
            partitionedRecords.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(record);
        }

        // Write each partition group to separate files
        for (Map.Entry<String, List<GenericRecord>> entry : partitionedRecords.entrySet()) {
            List<GenericRecord> partitionRecords = entry.getValue();

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
                    // Handle timestamp transforms (year, month, day, hour)
                    if (sourceValue instanceof Long) {
                        Long longValue = (Long) sourceValue;
                        // Apply the transform using Iceberg's transform API (fixed deprecated usage)
                        org.apache.iceberg.transforms.Transform<Long, Integer> transform =
                            (org.apache.iceberg.transforms.Transform<Long, Integer>) field.transform();
                        Integer partitionValue = transform.bind(table.schema().findType(field.sourceId())).apply(longValue);
                        partitionRecord.setField(partitionFieldName, partitionValue);
                    } else {
                        partitionRecord.setField(partitionFieldName, sourceValue);
                    }
                }
            }

            // Create proper data location for this partition using Iceberg's partition path structure
            org.apache.iceberg.StructLike partitionData = partitionRecord;
            String fileLocation = table.locationProvider().newDataLocation(table.spec(), partitionData, "data-" + UUID.randomUUID() + ".parquet");

            // Get the FileIO from the table (don't close it manually)
            org.apache.iceberg.io.FileIO fileIO = table.io();
            org.apache.iceberg.io.OutputFile outputFile = fileIO.newOutputFile(fileLocation);
            org.apache.iceberg.encryption.EncryptedOutputFile encryptedFile = table.encryption().encrypt(outputFile);

            // Create the data writer and write records
            org.apache.iceberg.io.DataWriter<org.apache.iceberg.data.Record> dataWriter =
                 factory.newDataWriter(encryptedFile, FileFormat.PARQUET, partitionRecord);

            try {
                // Write records for this partition
                for (GenericRecord partitionRec : partitionRecords) {
                    dataWriter.write(partitionRec);
                }
            } finally {
                // Always close the writer - this will complete the S3 upload
                dataWriter.close();
            }

            // Now we can safely get the result after closing
            org.apache.iceberg.io.DataWriteResult result = dataWriter.result();

            // Add all data files from this writer
            dataFiles.addAll(result.dataFiles());
        }

        System.out.println("✓ Written " + records.size() + " records with hierarchical partition structure");
        System.out.println("✓ Data files created with proper S3 path structure:");
        System.out.println("  - Records written: " + records.size());
        System.out.println("  - Data files: " + dataFiles.size());
        System.out.println("  - Expected S3 path: type=aig/tenant=<tenant>/year=<yyyy>/month=<mm>/day=<dd>/hour=<hh>/");

        // Add all data files to the append operation
        for (DataFile dataFile : dataFiles) {
            append.appendFile(dataFile);
        }

        // Atomic commit - makes all data visible at once
        append.commit();

        System.out.println("✓ Atomic commit completed - all data visible with hierarchical partition structure");
    }
}
