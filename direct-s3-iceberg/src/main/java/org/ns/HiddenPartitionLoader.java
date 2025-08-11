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

public class HiddenPartitionLoader {

    private static final int THREAD_POOL_SIZE = 4;
    private static final int FILES_PER_BATCH = 5;

    public static void main(String[] args) throws Exception {
        String localParquetDir = "/Users/vsrini/Downloads/parquet_files";

        String warehouse = "s3://ns-dpl-ice-poc/";
        String database = "dpl_events_ice";
        String tableName = "events";

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com");
        hadoopConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        catalogProps.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
        catalogProps.put("glue.skip-name-validation", "true");

        GlueCatalog catalog = new GlueCatalog();
        catalog.setConf(hadoopConf);
        catalog.initialize("glue", catalogProps);

        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        Namespace namespace = Namespace.of(database);

        if (!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace);
        }

        Table table = catalog.tableExists(tableId)
                ? catalog.loadTable(tableId)
                : createTableWithTyPartition(tableId, localParquetDir, hadoopConf, catalog);

        // Update table properties if needed
        table.updateProperties()
                .set(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name())
                .set("write.parquet.compression-codec", "zstd")
                .commit();

        File dir = new File(localParquetDir);
        File[] allFiles = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".parquet"));
        if (allFiles == null || allFiles.length == 0) {
            System.out.println("No parquet files found in " + localParquetDir);
            return;
        }

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

        AppendFiles append = table.newAppend();
        for (DataFile dataFile : allDataFiles) {
            append.appendFile(dataFile);
        }
        append.commit();

        System.out.println("✅ Successfully appended " + allDataFiles.size() + " parquet files to Iceberg table: " + tableId);
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
            // Remove any event_ts_* or partition fields that you want to clean up
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

            String filename = "data-" + UUID.randomUUID() + ".parquet";
            OutputFile outputFile = table.io().newOutputFile(table.locationProvider().newDataLocation(filename));

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
                            // Copy all fields except 'ty' because it is used as partition (optional)
                            if (!"ty".equals(name)) {
                                target.setField(name, src.getField(name));
                            } else {
                                // You may still want to include the 'ty' field in data if it exists
                                target.setField(name, src.getField(name));
                            }
                        }
                    }

                    writer.add(target);
                    recordCount++;
                }
            }

            long fileSizeInBytes = table.io().newInputFile(outputFile.location()).getLength();

            // Set partition data based on 'ty' field from the first record of the file
            // For simplicity, let's read the first record's 'ty' value from the original parquet file
            // You can optimize or customize this as needed

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
                // If no partition value is found, set null for partition
                for (int i = 0; i < partitionData.size(); i++) {
                    partitionData.set(i, null);
                }
            } else {
                // Set the partition string value
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
