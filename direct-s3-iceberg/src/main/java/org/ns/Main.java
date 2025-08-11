package org.ns;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        String parquetFilePath = "s3a://ns-nonprod-eng-data-pipeline-events-us-west-2-dev/type=alerts/tenants=10-260-65-520/year=2025/month=08/day=01/hour=17/1754070233_933ccf11-2a82-4784-8df0-5080fc18b646.parquet";
        String partitionField = "ty"; // Use a valid primitive field from your Parquet schema
        String warehousePath = "s3://ns-dpl-ice-poc/";
        String databaseName = "dpl_events_ice";
        String tableName = "events";

        // Step 1: Read Parquet schema
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        HadoopInputFile inputFile = HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(parquetFilePath), conf);
        ParquetFileReader reader = ParquetFileReader.open(inputFile);
        ParquetMetadata metadata = reader.getFooter();
        MessageType parquetSchema = metadata.getFileMetaData().getSchema();
        reader.close();

        // Convert Parquet schema to Iceberg
        org.apache.iceberg.Schema icebergSchema = ParquetSchemaUtil.convert(parquetSchema);// Step 2: Define Iceberg partition spec
        if (icebergSchema.findField(partitionField) == null) {
            throw new IllegalArgumentException("Partition field not found: " + partitionField);
        }
        PartitionSpec spec = PartitionSpec.builderFor(icebergSchema)
                .identity(partitionField)
                .build();

        // Step 3: Initialize Glue catalog
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);
        catalogProps.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
        catalogProps.put("glue.skip-name-validation", "true");

        GlueCatalog catalog = new GlueCatalog();
        catalog.setConf(conf);
        catalog.initialize("glue", catalogProps);

        TableIdentifier tableId = TableIdentifier.of(databaseName, tableName);
        Namespace namespace = Namespace.of(databaseName);
        if (!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace);
        }

        // Step 4: Create Iceberg table if not exists
        if (!catalog.tableExists(tableId)) {
            catalog.createTable(tableId, icebergSchema, spec);
            System.out.println("Iceberg table created at " + warehousePath);
        } else {
            System.out.println("Table already exists: " + tableId);
        }
    }
}
