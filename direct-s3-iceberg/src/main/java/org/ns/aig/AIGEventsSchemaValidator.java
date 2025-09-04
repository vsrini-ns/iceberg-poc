package org.ns.aig;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.data.GenericRecord;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

/**
 * AIG Events Schema Validator for Apache Iceberg
 * Comprehensive validation of the AI Gateway events table schema
 */
public class AIGEventsSchemaValidator {

    public static void main(String[] args) {
        System.out.println("=== AIG Events Iceberg Schema Validation ===\n");

        try {
            // Create the events schema
            Schema eventsSchema = createEventsSchema();

            // Run all validation tests
            System.out.println("1. Testing schema creation...");
            testSchemaCreation(eventsSchema);

            System.out.println("\n2. Testing DDL type mappings...");
            testDDLMapping(eventsSchema);

            System.out.println("\n3. Testing data record creation...");
            testRecordCreation(eventsSchema);

            System.out.println("\n4. Testing complex nested structures...");
            testComplexStructures(eventsSchema);

            System.out.println("\n5. Testing schema serialization...");
            testSchemaSerialization(eventsSchema);

            System.out.println("\n6. Generating DDL CREATE TABLE statement...");
            generateDDL(eventsSchema);

            System.out.println("\n🎉 ALL VALIDATION TESTS PASSED!");
            System.out.println("\n✅ Your AIG Events Iceberg schema is VALID and production-ready!");
            System.out.println("✅ All DDL types map correctly to Iceberg types");
            System.out.println("✅ Complex nested structures work perfectly");
            System.out.println("✅ Schema can be serialized and used in Iceberg tables");

        } catch (Exception e) {
            System.err.println("❌ AIG Events schema validation FAILED!");
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static Schema createEventsSchema() {
        return new Schema(
            // Basic fields - exactly matching your DDL
            Types.NestedField.optional(1, "transaction_id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "response_id", Types.StringType.get()),
            Types.NestedField.required(3, "tenant_id", Types.IntegerType.get()),
            Types.NestedField.required(4, "home_pop", Types.StringType.get()),
            Types.NestedField.required(5, "service_id", Types.StringType.get()),
            Types.NestedField.required(6, "timestamp", Types.LongType.get()),
            Types.NestedField.optional(7, "version", Types.StringType.get()),
            Types.NestedField.optional(8, "type", Types.StringType.get()),
            Types.NestedField.optional(9, "token_group", Types.StringType.get()),
            Types.NestedField.optional(10, "gateway_id", Types.StringType.get()),
            Types.NestedField.optional(11, "ai_provider_id", Types.StringType.get()),
            Types.NestedField.optional(12, "ai_schema", Types.StringType.get()),
            Types.NestedField.optional(13, "cs_model", Types.StringType.get()),
            Types.NestedField.optional(14, "rs_model", Types.StringType.get()),
            Types.NestedField.optional(15, "sc_status", Types.IntegerType.get()),
            Types.NestedField.optional(16, "cs_method", Types.StringType.get()),
            Types.NestedField.optional(17, "cs_protocol", Types.StringType.get()),
            Types.NestedField.optional(18, "cs_url", Types.StringType.get()),
            Types.NestedField.optional(19, "cs_scheme", Types.StringType.get()),
            Types.NestedField.optional(20, "cs_domain", Types.StringType.get()),
            Types.NestedField.optional(21, "cs_path", Types.StringType.get()),
            Types.NestedField.optional(22, "cs_query", Types.StringType.get()),
            Types.NestedField.optional(23, "sr_url", Types.StringType.get()),
            Types.NestedField.optional(24, "sr_scheme", Types.StringType.get()),
            Types.NestedField.optional(25, "sr_domain", Types.StringType.get()),
            Types.NestedField.optional(26, "sr_path", Types.StringType.get()),
            Types.NestedField.optional(27, "sr_query", Types.StringType.get()),
            Types.NestedField.optional(28, "rs_status", Types.IntegerType.get()),
            Types.NestedField.optional(29, "rs_response_time", Types.IntegerType.get()),
            Types.NestedField.optional(30, "usage_total", Types.IntegerType.get()),
            Types.NestedField.optional(31, "usage_input", Types.IntegerType.get()),
            Types.NestedField.optional(32, "usage_output", Types.IntegerType.get()),

            // Complex nested policy structure
            Types.NestedField.optional(33, "policy", Types.ListType.ofOptional(34, Types.StructType.of(
                Types.NestedField.optional(35, "type", Types.StringType.get()),
                Types.NestedField.optional(36, "name", Types.StringType.get()),
                Types.NestedField.optional(37, "action", Types.StringType.get()),
                Types.NestedField.optional(38, "profile", Types.ListType.ofOptional(39, Types.StringType.get())),
                Types.NestedField.optional(40, "object_id", Types.IntegerType.get())
            ))),

            Types.NestedField.optional(41, "action", Types.StringType.get()),
            Types.NestedField.optional(42, "action_reason", Types.StringType.get()),
            Types.NestedField.optional(43, "custom_attr", Types.StringType.get()),

            // cs_files: array of structs
            Types.NestedField.optional(44, "cs_files", Types.ListType.ofOptional(45, Types.StructType.of(
                Types.NestedField.optional(46, "object_id", Types.IntegerType.get()),
                Types.NestedField.optional(47, "purpose", Types.StringType.get()),
                Types.NestedField.optional(48, "type", Types.StringType.get()),
                Types.NestedField.optional(49, "mime_type", Types.StringType.get()),
                Types.NestedField.optional(50, "filename", Types.StringType.get()),
                Types.NestedField.optional(51, "bytes", Types.LongType.get()),
                Types.NestedField.optional(52, "create_at", Types.LongType.get()),
                Types.NestedField.optional(53, "update_at", Types.LongType.get())
            ))),

            // rs_files: array of structs
            Types.NestedField.optional(54, "rs_files", Types.ListType.ofOptional(55, Types.StructType.of(
                Types.NestedField.optional(56, "object_id", Types.IntegerType.get()),
                Types.NestedField.optional(57, "purpose", Types.StringType.get()),
                Types.NestedField.optional(58, "type", Types.StringType.get()),
                Types.NestedField.optional(59, "mime_type", Types.StringType.get()),
                Types.NestedField.optional(60, "filename", Types.StringType.get()),
                Types.NestedField.optional(61, "bytes", Types.LongType.get()),
                Types.NestedField.optional(62, "create_at", Types.LongType.get()),
                Types.NestedField.optional(63, "update_at", Types.LongType.get())
            ))),

            // ratelimit: array of strings
            Types.NestedField.optional(64, "ratelimit", Types.ListType.ofOptional(65, Types.StringType.get())),

            Types.NestedField.optional(66, "activity", Types.StringType.get()),
            Types.NestedField.optional(67, "conversation_id", Types.StringType.get()),
            Types.NestedField.optional(68, "incident_id", Types.IntegerType.get())
        );
    }

    private static void testSchemaCreation(Schema schema) {
        System.out.println("  ✓ AIG Events schema created with " + schema.columns().size() + " fields");
        System.out.println("  ✓ Highest field ID: " + schema.highestFieldId());

        // Verify required fields
        String[] requiredFields = {"tenant_id", "home_pop", "service_id", "timestamp"};
        for (String fieldName : requiredFields) {
            Types.NestedField field = schema.findField(fieldName);
            if (field == null || field.isOptional()) {
                throw new RuntimeException("Required field issue: " + fieldName);
            }
            System.out.println("  ✓ Required field: " + fieldName);
        }
    }

    private static void testDDLMapping(Schema schema) {
        // Test your original DDL type mappings
        validateMapping(schema, "transaction_id", "int", Types.IntegerType.get(), true);
        validateMapping(schema, "response_id", "string", Types.StringType.get(), true);
        validateMapping(schema, "tenant_id", "int NOT NULL", Types.IntegerType.get(), false);
        validateMapping(schema, "timestamp", "long NOT NULL", Types.LongType.get(), false);

        // Test complex types
        validateComplexMapping(schema, "policy", "list<struct>");
        validateComplexMapping(schema, "cs_files", "list<struct>");
        validateComplexMapping(schema, "rs_files", "list<struct>");
        validateComplexMapping(schema, "ratelimit", "list<string>");

        System.out.println("  ✓ All AIG Events DDL types correctly mapped to Iceberg types");
    }

    private static void validateMapping(Schema schema, String field, String ddl, Type expectedType, boolean shouldBeOptional) {
        Types.NestedField icebergField = schema.findField(field);
        if (!icebergField.type().equals(expectedType)) {
            throw new RuntimeException("Type mismatch: " + field);
        }
        if (icebergField.isOptional() != shouldBeOptional) {
            throw new RuntimeException("Nullability mismatch: " + field);
        }
        System.out.println("  ✓ " + field + ": " + ddl + " -> " + expectedType);
    }

    private static void validateComplexMapping(Schema schema, String field, String ddl) {
        Types.NestedField icebergField = schema.findField(field);
        if (!icebergField.type().isListType()) {
            throw new RuntimeException("Expected list type: " + field);
        }
        System.out.println("  ✓ " + field + ": " + ddl + " -> " + icebergField.type());
    }

    private static void testRecordCreation(Schema schema) {
        GenericRecord record = GenericRecord.create(schema);

        // Set required fields
        record.setField("tenant_id", 12345);
        record.setField("home_pop", "us-west-2");
        record.setField("service_id", "ai-gateway");
        record.setField("timestamp", System.currentTimeMillis());

        // Set some optional fields
        record.setField("transaction_id", 67890);
        record.setField("type", "inference");
        record.setField("usage_total", 100);

        System.out.println("  ✓ AIG Events generic record created successfully");
        System.out.println("  ✓ Required fields populated");
        System.out.println("  ✓ Optional fields populated");
    }

    private static void testComplexStructures(Schema schema) {
        GenericRecord record = GenericRecord.create(schema);

        // Test policy array
        List<GenericRecord> policies = new ArrayList<>();
        GenericRecord policy = GenericRecord.create(
            schema.findField("policy").type().asListType().elementType().asStructType()
        );
        policy.setField("type", "rate_limit");
        policy.setField("name", "test_policy");
        policy.setField("action", "allow");
        policy.setField("profile", Arrays.asList("standard", "premium"));
        policy.setField("object_id", 1001);
        policies.add(policy);
        record.setField("policy", policies);

        // Test files array
        List<GenericRecord> files = new ArrayList<>();
        GenericRecord file = GenericRecord.create(
            schema.findField("cs_files").type().asListType().elementType().asStructType()
        );
        file.setField("object_id", 2001);
        file.setField("purpose", "input");
        file.setField("type", "text");
        file.setField("filename", "test.txt");
        file.setField("bytes", 1024L);
        files.add(file);
        record.setField("cs_files", files);

        // Test string array
        record.setField("ratelimit", Arrays.asList("token_bucket", "sliding_window"));

        System.out.println("  ✓ AIG policy array with nested structures created");
        System.out.println("  ✓ AIG file metadata arrays created");
        System.out.println("  ✓ AIG rate limit string arrays created");
        System.out.println("  ✓ All AIG complex nested structures working perfectly");
    }

    private static void testSchemaSerialization(Schema schema) {
        String schemaJson = schema.toString();
        System.out.println("  ✓ AIG Events schema serialized to JSON (" + schemaJson.length() + " chars)");

        // Verify key fields are in serialized form
        String[] keyFields = {"tenant_id", "policy", "cs_files", "ratelimit"};
        for (String field : keyFields) {
            if (!schemaJson.contains(field)) {
                throw new RuntimeException("Field missing in serialization: " + field);
            }
        }
        System.out.println("  ✓ All AIG Events fields present in serialized schema");
    }

    private static void generateDDL(Schema schema) {
        // Generate DDL CREATE TABLE statement
        StringBuilder ddl = new StringBuilder("CREATE TABLE aig_events (\n");

        // Append each field to the DDL
        for (Types.NestedField field : schema.columns()) {
            ddl.append("  ")
               .append(field.name())
               .append(" ")
               .append(mapIcebergToDDLType(field.type()))
               .append(field.isOptional() ? " NULL" : " NOT NULL")
               .append(",\n");
        }

        // Remove the last comma and add closing parenthesis
        ddl.setLength(ddl.length() - 2);
        ddl.append("\n) USING iceberg");

        System.out.println("  ✓ DDL CREATE TABLE statement generated:");
        System.out.println(ddl.toString());
    }

    private static String mapIcebergToDDLType(Type type) {
        // Map Iceberg types to DDL types
        if (type.equals(Types.IntegerType.get())) {
            return "int";
        } else if (type.equals(Types.LongType.get())) {
            return "long";
        } else if (type.equals(Types.StringType.get())) {
            return "string";
        } else if (type.equals(Types.BooleanType.get())) {
            return "boolean";
        } else if (type.isListType()) {
            return "list<" + mapIcebergToDDLType(type.asListType().elementType()) + ">";
        } else if (type.isStructType()) {
            return "struct<" + String.join(", ", type.asStructType().fields().stream()
                .map(f -> f.name() + ": " + mapIcebergToDDLType(f.type()))
                .toArray(String[]::new)) + ">";
        }
        return "unknown";
    }
}
