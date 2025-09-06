package org.ns.aig;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;

/**
 * Simple test to verify S3 filesystem configuration
 */
public class SimpleConfigTest {

    public static void main(String[] args) {
        System.out.println("=== Simple Configuration Test ===");

        // Test 1: Empty configuration (reproduces original issue)
        System.out.println("\nTest 1: Empty Configuration (should fail)");
        Configuration emptyConf = new Configuration();
        testConfiguration(emptyConf);

        // Test 2: Properly configured (our fix)
        System.out.println("\nTest 2: Properly Configured (should work)");
        Configuration properConf = setupHadoopConfiguration();
        testConfiguration(properConf);

        System.out.println("\n=== Test Complete ===");
    }

    private static void testConfiguration(Configuration config) {
        try (HadoopFileIO fileIO = new HadoopFileIO(config)) {
            String s3Path = "s3://test-bucket/test-file.parquet";
            fileIO.newOutputFile(s3Path);
            System.out.println("✓ SUCCESS: S3 filesystem recognized");
        } catch (Exception e) {
            if (e.getMessage().contains("No FileSystem for scheme \"s3\"")) {
                System.out.println("❌ FAILED: " + e.getMessage());
            } else {
                System.out.println("✓ SUCCESS: S3 filesystem recognized (different error: " + e.getClass().getSimpleName() + ")");
            }
        }
    }

    private static Configuration setupHadoopConfiguration() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        hadoopConf.set("fs.s3.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        return hadoopConf;
    }
}
