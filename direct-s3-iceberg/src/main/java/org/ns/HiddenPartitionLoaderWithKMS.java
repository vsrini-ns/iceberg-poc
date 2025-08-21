package org.ns;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.*;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

public class HiddenPartitionLoaderWithKMS {

    private static final int THREAD_POOL_SIZE = 4;
    private static final int FILES_PER_BATCH = 5;

    // ---------- Configure these for your run ----------
    private static final String LOCAL_PARQUET_DIR = "/Users/vsrini/Downloads/parquet_files_events";
    private static final String WAREHOUSE = "s3://ns-dpl-ice-poc/";
    private static final String DATABASE = "dpl_events_ice";
    private static final String TABLE_NAME = "events";

    // TENANT values (provide per run)
    private static final String TENANT_ID = "1234";
    private static final String TENANT_KMS_KEY_ARN = "arn:aws:kms:us-west-2:205930614247:key/df94b771-5ca3-4479-8250-559f04a7bd2e";

    // S3A endpoint (optional; keep if you use a specific region/endpoint)
    private static final String S3A_ENDPOINT = "s3.us-west-2.amazonaws.com";
    // -------------------------------------------------

    public static void main(String[] args) throws Exception {
        // Hadoop configuration (S3A + KMS)
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.endpoint", S3A_ENDPOINT);
        hadoopConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

        // Enforce SSE-KMS with tenant-specific key for writes performed by S3A
        hadoopConf.set("fs.s3a.server-side-encryption-algorithm", "SSE-KMS");
        // IMPORTANT: this is per-process setting. If you need multiple tenant keys concurrently,
        // see note at top of file.
        hadoopConf.set("fs.s3a.server-side-encryption.key", TENANT_KMS_KEY_ARN);

        // Iceberg catalog props (GlueCatalog)
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE);
        catalogProps.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
        // Tell Iceberg's S3FileIO to use KMS type and the key (keeps config explicit)
        catalogProps.put("s3.sse.type", "kms");
        catalogProps.put("s3.sse.key", TENANT_KMS_KEY_ARN);
        catalogProps.put("glue.skip-name-validation", "true");

        // Initialize Glue catalog
        GlueCatalog catalog = new GlueCatalog();
        catalog.setConf(hadoopConf);
        catalog.initialize("glue", catalogProps);

        TableIdentifier tableId = TableIdentifier.of(DATABASE, TABLE_NAME);
        Namespace namespace = Namespace.of(DATABASE);

        if (!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace);
        }

        // Load or create table (schema inference from sample parquet)
        Table table = catalog.tableExists(tableId)
                ? catalog.loadTable(tableId)
                : createTableWithTyPartition(tableId, LOCAL_PARQUET_DIR, hadoopConf, catalog);

        // Ensure table write props
        table.updateProperties()
                .set(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name())
                .set("write.parquet.compression-codec", "zstd")
                .commit();

        // Find local parquet files
        File dir = new File(LOCAL_PARQUET_DIR);
        File[] allFiles = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".parquet"));
        if (allFiles == null || allFiles.length == 0) {
            System.out.println("No parquet files found in " + LOCAL_PARQUET_DIR);
            return;
        }

        // Partition into batches and process concurrently
        List<List<File>> batches = partitionFiles(Arrays.asList(allFiles), FILES_PER_BATCH);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<Future<List<DataFile>>> futures = new ArrayList<>();

        for (List<File> batch : batches) {
            futures.add(executor.submit(() -> processBatch(batch, table, hadoopConf)));
        }

        List<DataFile> allDataFiles = new ArrayList<>();
        for (Future<List<DataFile>> future : futures) {
            try {
                allDataFiles.addAll(future.get());
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        executor.shutdown();

        // Append all produced data files to Iceberg table
        AppendFiles append = table.newAppend();
        for (DataFile dataFile : allDataFiles) {
            append.appendFile(dataFile);
        }
        append.commit();

        System.out.println("✅ Successfully appended " + allDataFiles.size() +
                " parquet files to Iceberg table: " + tableId +
                " using SSE-KMS key: " + TENANT_KMS_KEY_ARN);
    }

    private static Table createTableWithTyPartition(TableIdentifier tableId,
                                                    String localParquetDir,
                                                    Configuration hadoopConf,
                                                    GlueCatalog catalog) throws Exception {
        // Pick a sample parquet file for schema inference
        File sampleFile = Arrays.stream(Objects.requireNonNull(new File(localParquetDir)
                        .listFiles((d, name) -> name.toLowerCase().endsWith(".parquet"))))
                .findFirst().orElseThrow(() -> new RuntimeException("No parquet files found"));

        org.apache.parquet.hadoop.util.HadoopInputFile parquetInputFile =
                org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(new Path(sampleFile.getAbsolutePath()), hadoopConf);

        ParquetMetadata meta = ParquetFileReader.open(parquetInputFile).getFooter();
        MessageType parquetSchema = meta.getFileMetaData().getSchema();
        Schema icebergSchema = ParquetSchemaUtil.convert(parquetSchema);

        // Remove any existing partition fields that might cause conflict (e.g., old ts partitions)
        List<Types.NestedField> cleanedFields = new ArrayList<>();
        for (Types.NestedField field : icebergSchema.columns()) {
            String name = field.name();
            if (!name.startsWith("event_ts_") && !name.equals("event_ts")) {
                cleanedFields.add(field);
            }
        }
        icebergSchema = new Schema(cleanedFields);

        // Build partition spec using "ty" string field (identity partition)
        PartitionSpec spec = PartitionSpec.builderFor(icebergSchema)
                .identity("ty")
                .build();

        System.out.println("Creating Iceberg table with schema: " + icebergSchema);
        System.out.println("Using partition spec on 'ty': " + spec);

        return catalog.createTable(tableId, icebergSchema, spec);
    }

    private static List<DataFile> processBatch(List<File> batch, Table table, Configuration hadoopConf) throws Exception {
        List<DataFile> produced = new ArrayList<>();
        for (File parquetFile : batch) {
            try {
                produced.add(processSingleParquetToIceberg(parquetFile, table, hadoopConf));
            } catch (Exception e) {
                System.err.println("Failed file " + parquetFile.getName() + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
        return produced;
    }

    private static DataFile processSingleParquetToIceberg(File parquetFile, Table table, Configuration hadoopConf) throws Exception {
        // Local input file for reading the source parquet
        InputFile icebergInputFile = Files.localInput(parquetFile);

        org.apache.parquet.hadoop.util.HadoopInputFile hif =
                org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(new Path(parquetFile.getAbsolutePath()), hadoopConf);
        ParquetMetadata meta = ParquetFileReader.open(hif).getFooter();
        MessageType parquetMessageType = meta.getFileMetaData().getSchema();
        Schema sourceSchema = ParquetSchemaUtil.convert(parquetMessageType);

        try (CloseableIterable<Record> records = org.apache.iceberg.parquet.Parquet.read(icebergInputFile)
                .project(sourceSchema)
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                .build()) {

            // Build the tenant-specific S3 object key (Iceberg-managed data location)
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            // Use current UTC time for path segmentation; change as needed
            int year = cal.get(Calendar.YEAR);
            int month = cal.get(Calendar.MONTH) + 1;
            int day = cal.get(Calendar.DAY_OF_MONTH);
            int hour = cal.get(Calendar.HOUR_OF_DAY);

            String filename = UUID.randomUUID().toString() + ".parquet";
            String s3RelativePath = String.format("tenants=%s/year=%04d/month=%02d/day=%02d/hour=%02d/%s",
                    TENANT_ID, year, month, day, hour, filename);

            // Iceberg-managed OutputFile (this will stream directly to S3 via Iceberg's FileIO)
            OutputFile outputFile = table.io().newOutputFile(table.locationProvider().newDataLocation(s3RelativePath));

            GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), table.spec())
                    .setAll(table.properties())
                    .set(TableProperties.PARQUET_COMPRESSION, "zstd");

            long recordCount = 0L;
            PartitionSpec spec = table.spec();
            PartitionData partitionData = new PartitionData(spec.partitionType());

            try (FileAppender<Record> writer = appenderFactory.newAppender(outputFile, FileFormat.PARQUET)) {
                for (Record src : records) {
                    GenericRecord target = GenericRecord.create(table.schema());

                    for (Types.NestedField f : sourceSchema.columns()) {
                        String name = f.name();
                        if (table.schema().findField(name) != null) {
                            // copy fields (including ty if present)
                            target.setField(name, src.getField(name));
                        }
                    }

                    writer.add(target);
                    recordCount++;
                }
            }

            long fileSizeInBytes = table.io().newInputFile(outputFile.location()).getLength();

            // Determine partition value 'ty' from first record of source file (if present)
            String tyPartitionValue = null;
            try (CloseableIterable<Record> recordsForPartition = org.apache.iceberg.parquet.Parquet.read(icebergInputFile)
                    .project(sourceSchema)
                    .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                    .build()) {
                Iterator<Record> iterator = recordsForPartition.iterator();
                if (iterator.hasNext()) {
                    Record firstRecord = iterator.next();
                    Object val = firstRecord.getField("ty");
                    if (val != null) {
                        tyPartitionValue = val.toString();
                    }
                }
            }

            if (tyPartitionValue == null) {
                for (int i = 0; i < partitionData.size(); i++) {
                    partitionData.set(i, null);
                }
            } else {
                partitionData.set(0, tyPartitionValue);
            }

            return DataFiles.builder(spec)
                    .withPath(outputFile.location())
                    .withFileSizeInBytes(fileSizeInBytes)
                    .withRecordCount(recordCount)
                    .withFormat(FileFormat.PARQUET)
                    .withPartition(partitionData)
                    .build();
        }
    }

    private static List<List<File>> partitionFiles(List<File> files, int batchSize) {
        List<List<File>> batches = new ArrayList<>();
        for (int i = 0; i < files.size(); i += batchSize) {
            batches.add(files.subList(i, Math.min(i + batchSize, files.size())));
        }
        return batches;
    }
}
