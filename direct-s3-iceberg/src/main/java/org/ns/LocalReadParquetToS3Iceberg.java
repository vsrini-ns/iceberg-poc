package org.ns;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.*;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class LocalReadParquetToS3Iceberg {

    public static void main(String[] args) throws Exception {
        // === Configuration ===
        String localParquetPath = "/Users/vsrini/Downloads/1754105273_a1ec8219-f5ac-4aa2-af27-e0b139a2d449.parquet";
        String partitionField = "ts";  // Partition column
        String warehouse = "s3://ns-dpl-ice-poc/";
        String database = "dpl_events_ice";
        String tableName = "events";

        // === Read schema from Parquet ===
        org.apache.parquet.io.InputFile parquetInputFile = new LocalParquetInputFile(localParquetPath);
        ParquetMetadata meta = ParquetFileReader.open(parquetInputFile).getFooter();
        MessageType parquetSchema = meta.getFileMetaData().getSchema();
        Schema icebergSchema = ParquetSchemaUtil.convert(parquetSchema);

        if (icebergSchema.findField(partitionField) == null) {
            throw new IllegalArgumentException("Partition field not found: " + partitionField);
        }

        // === Partition specification ===
        PartitionSpec spec = PartitionSpec.builderFor(icebergSchema)
                .identity(partitionField)
                .build();

        // === Hadoop/S3 config ===
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com");

        // === Glue Catalog ===
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
                : catalog.createTable(tableId, icebergSchema, spec);

        // === Set ZSTD compression ===
        table.updateProperties()
                .set(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name())
                .set("write.parquet.compression-codec", "zstd")
                .commit();

        // === Read records from local Parquet ===
        org.apache.iceberg.io.InputFile icebergInputFile = Files.localInput(new File(localParquetPath));
        CloseableIterable<Record> records = Parquet.read(icebergInputFile)
                .project(table.schema())
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                .build();

        // === Prepare output file ===
        String filename = "data-" + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(table.locationProvider().newDataLocation(filename));

        GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), spec).setAll(table.properties());

        long recordCount = 0L;
        long fileSizeInBytes;
        DataFile dataFile;

        // === Extract partition data ===
        PartitionData partitionData = new PartitionData(spec.partitionType());
        boolean partitionSet = false;

        try (
                FileAppender<Record> writer = appenderFactory.newAppender(outputFile, FileFormat.PARQUET);
                CloseableIterable<Record> closeableRecords = records
        ) {
            for (Record record : closeableRecords) {
                writer.add(record);
                recordCount++;

                if (!partitionSet) {
                    Object partitionValue = record.getField(partitionField);
                    partitionData.set(0, partitionValue);  // 0 = first partition field
                    partitionSet = true;
                }
            }

            writer.close();  // Write footer
            fileSizeInBytes = writer.length();

            dataFile = DataFiles.builder(spec)
                    .withPath(outputFile.location())
                    .withFileSizeInBytes(fileSizeInBytes)
                    .withRecordCount(recordCount)
                    .withFormat(FileFormat.PARQUET)
                    .withPartition(partitionData) // ✅ Important!
                    .build();
        }

        // === Commit data file to Iceberg ===
        table.newAppend().appendFile(dataFile).commit();
        System.out.println("✅ Data successfully loaded into Iceberg table: " + tableId);
    }

    // === Helper: Parquet InputFile for local files ===
    static class LocalParquetInputFile implements org.apache.parquet.io.InputFile {
        private final File file;

        public LocalParquetInputFile(String path) {
            this.file = new File(path);
        }

        @Override
        public long getLength() {
            return file.length();
        }

        @Override
        public SeekableInputStream newStream() {
            try {
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                return new DelegatingSeekableInputStream(Channels.newInputStream(raf.getChannel())) {
                    @Override
                    public long getPos() {
                        try {
                            return raf.getFilePointer();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void seek(long newPos) {
                        try {
                            raf.seek(newPos);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void close() {
                        try {
                            raf.close();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
