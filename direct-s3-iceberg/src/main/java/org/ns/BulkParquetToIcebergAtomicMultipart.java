package org.ns;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.*;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Types;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.time.ZoneId;

public class BulkParquetToIcebergAtomicMultipart {

    private static final int THREAD_POOL_SIZE = 4;
    private static final int FILES_PER_BATCH = 5;

    public static void main(String[] args) throws Exception {
        String localParquetDir = "/Users/vsrini/Downloads/parquet_files";
        List<String> partitionFields = List.of("year", "month", "day");
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
                : createTableFromSchema(tableId, localParquetDir, partitionFields, hadoopConf, catalog);

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
            futures.add(executor.submit(() -> processBatch(batch, table, partitionFields)));
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

    private static Table createTableFromSchema(TableIdentifier tableId, String localParquetDir,
                                               List<String> partitionFields, Configuration hadoopConf,
                                               GlueCatalog catalog) throws Exception {
        File sampleFile = Arrays.stream(new File(localParquetDir)
                        .listFiles((d, name) -> name.endsWith(".parquet")))
                .findFirst().orElseThrow(() -> new RuntimeException("No parquet files found"));

        org.apache.parquet.hadoop.util.HadoopInputFile parquetInputFile =
                org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(new Path(sampleFile.getAbsolutePath()), hadoopConf);

        ParquetMetadata meta = ParquetFileReader.open(parquetInputFile).getFooter();
        MessageType parquetSchema = meta.getFileMetaData().getSchema();
        Schema icebergSchema = ParquetSchemaUtil.convert(parquetSchema);

        List<Types.NestedField> fields = new ArrayList<>(icebergSchema.columns());
        for (String p : partitionFields) {
            if (icebergSchema.findField(p) == null) {
                fields.add(Types.NestedField.optional(fields.size() + 1, p, Types.IntegerType.get()));
            }
        }
        icebergSchema = new Schema(fields);

        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(icebergSchema);
        partitionFields.forEach(specBuilder::identity);
        PartitionSpec spec = specBuilder.build();

        return catalog.createTable(tableId, icebergSchema, spec);
    }

    private static List<DataFile> processBatch(List<File> batch, Table table, List<String> partitionFields) throws Exception {
        List<DataFile> dataFiles = new ArrayList<>();
        for (File parquetFile : batch) {
            dataFiles.add(processSingleParquetWithPartitions(parquetFile, table, partitionFields));
        }
        return dataFiles;
    }

    private static DataFile processSingleParquetWithPartitions(File parquetFile, Table table, List<String> partitionFields) throws Exception {
        InputFile icebergInputFile = Files.localInput(parquetFile);

        try (CloseableIterable<Record> records = org.apache.iceberg.parquet.Parquet.read(icebergInputFile)
                .project(table.schema())
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                .build()) {

            String filename = "data-" + UUID.randomUUID() + ".parquet";
            OutputFile outputFile = table.io().newOutputFile(table.locationProvider().newDataLocation(filename));
            // GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), table.spec());
            GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), table.spec())
                    .setAll(table.properties())
                    .set(TableProperties.PARQUET_COMPRESSION, "zstd");

            long recordCount = 0L;
            PartitionSpec spec = table.spec();
            PartitionData partitionData = new PartitionData(spec.partitionType());

            LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
            List<Types.NestedField> partitionTypeFields = spec.partitionType().fields();

            for (int i = 0; i < partitionTypeFields.size(); i++) {
                String name = partitionTypeFields.get(i).name();
                if ("year".equals(name)) {
                    partitionData.set(i, now.getYear());
                } else if ("month".equals(name)) {
                    partitionData.set(i, now.getMonthValue());
                } else if ("day".equals(name)) {
                    partitionData.set(i, now.getDayOfMonth());
                } else {
                    partitionData.set(i, null);
                }
            }

            try (FileAppender<Record> writer = appenderFactory.newAppender(outputFile, FileFormat.PARQUET)) {
                for (Record record : records) {
                    writer.add(record);
                    recordCount++;
                }
            }

            long fileSizeInBytes = table.io().newInputFile(outputFile.location()).getLength();

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
