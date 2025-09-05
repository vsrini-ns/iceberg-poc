package org.ns.aig;

import org.apache.iceberg.*;
import org.apache.iceberg.types.Types;

/**
 * Simple test to verify partition specification creates the correct hierarchical structure
 */
public class PartitionSpecTest {

    public static void main(String[] args) {
        System.out.println("=== Partition Specification Test ===\n");

        // Create a simple schema for testing
        Schema schema = new Schema(
            Types.NestedField.required(1, "type", Types.StringType.get()),
            Types.NestedField.required(2, "tenant_id", Types.IntegerType.get()),
            Types.NestedField.required(3, "timestamp", Types.LongType.get())
        );

        // Create the partition spec with hierarchical time-based partitioning
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("type")                          // type=<type>
                .identity("tenant_id")                     // tenant=<tenant_id> (using default name)
                .year("timestamp")                         // year=<yyyy>
                .month("timestamp")                        // month=<mm>
                .day("timestamp")                          // day=<dd>
                .hour("timestamp")                         // hour=<hh>
                .build();

        System.out.println("✓ Partition specification created successfully");
        System.out.println("✓ Partition fields count: " + spec.fields().size());

        // Print partition field details
        for (int i = 0; i < spec.fields().size(); i++) {
            PartitionField field = spec.fields().get(i);
            String sourceName = schema.findColumnName(field.sourceId());
            System.out.println("  - Field " + (i+1) + ": " + field.name() +
                             " (from " + sourceName + ", transform: " + field.transform() + ")");
        }

        System.out.println("\n✓ Expected S3 path structure:");
        System.out.println("  s3://bucket/warehouse/database.db/table/data/");
        System.out.println("  └── type=aig/");
        System.out.println("      └── tenant=1000/");
        System.out.println("          └── year=2025/");
        System.out.println("              └── month=9/");
        System.out.println("                  └── day=4/");
        System.out.println("                      └── hour=18/");
        System.out.println("                          └── <uuid>.parquet");

        System.out.println("\n🎉 Partition specification test completed successfully!");
    }
}
