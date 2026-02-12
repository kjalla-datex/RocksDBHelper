# RocksDBHelper

A Java utility for analyzing RocksDB databases to identify and export orphaned index entries. This tool scans RocksDB shards containing cabinet and index data, and generates CSV reports of indexes that are not associated with any existing cabinet entries.

## Overview

The RocksDBHelper project is designed to work with encrypted RocksDB databases that store:
- **Cabinet data**: Contains the main data entries with UUID identifiers
- **Index data**: Contains index entries that should reference cabinet entries

The tool identifies "orphan indexes" - index entries that do not have corresponding cabinet entries, which may indicate data inconsistencies or cleanup opportunities.

## Features

- **Encrypted Data Support**: Handles AES-encrypted RocksDB entries
- **Progress Logging**: Provides timestamped logging with detailed progress information
- **Configurable Limits**: Set maximum number of orphan entries to export
- **CSV Export**: Generates structured CSV reports for analysis
- **Multi-Shard Support**: Processes multiple RocksDB shards across different folders

## Prerequisites

- Java 8 or higher
- Maven 3.6+
- Access to RocksDB data files

## Configuration

Create or modify the `rocks-exporter.properties` file with your environment settings:

```properties
# Device UUID for encryption key derivation
DEVICE_UUID=4E85DCEB-5BBF-5BF6-9B92-54EFEBF98724

# Paths to RocksDB data directories
INDEX_BASE=/path/to/your/index/data
CABINET_BASE=/path/to/your/cabinet/data

# Output directory for CSV reports
OUTPUT_DIR=csv_dumps

# Maximum number of orphan entries to export (default: 10)
DEFAULT_ORPHAN_LIMIT=250

# Index entry prefix to identify index records (default: dbidxEntry)
INDEX_PREFIX=dbidxEntry
```

## Building the Fat JAR

The project is configured to create a "fat JAR" that includes all dependencies, making it easy to distribute and run.

### Build Command

```bash
mvn clean package
```

This will create two JAR files in the `target/` directory:
- `RocksDBHelper-1.0-SNAPSHOT.jar` - The fat JAR (~6.8MB) with all dependencies
- `original-RocksDBHelper-1.0-SNAPSHOT.jar` - The original JAR without dependencies (~17KB)

### Build Output

After a successful build, you'll see output similar to:
```
[INFO] Building jar: /path/to/RocksDBHelper/target/RocksDBHelper-1.0-SNAPSHOT.jar
[INFO] Including org.rocksdb:rocksdbjni:jar:4.8.0 in the shaded jar.
[INFO] Including org.apache.thrift:libthrift:jar:0.9.1 in the shaded jar.
[INFO] BUILD SUCCESS
```

### Maven Dependency Issues

If you encounter Maven dependency resolution errors like:
```
Could not transfer artifact org.codehaus.plexus:plexus-utils:jar:3.5.1 from/to...
```

Follow these steps to resolve:

1. **Clear corrupted Maven cache:**
   ```bash
   rm -rf ~/.m2/repository/org/apache/maven/plugins/maven-resources-plugin
   rm -rf ~/.m2/repository/org/codehaus/plexus
   ```

2. **Clean and rebuild:**
   ```bash
   mvn clean -U
   mvn compile
   mvn package
   ```

3. **Alternative approach - purge specific dependencies:**
   ```bash
   mvn dependency:purge-local-repository -DmanualInclude="org.codehaus.plexus:plexus-utils"
   ```

## Running the Application

### Basic Usage

Run with the default configuration file:
```bash
java -jar target/RocksDBHelper-1.0-SNAPSHOT.jar
```

### Custom Configuration File

Specify a custom properties file:
```bash
java -jar target/RocksDBHelper-1.0-SNAPSHOT.jar /path/to/custom-config.properties
```

### Example Output

When running, you'll see timestamped logging output:
```
[2026-02-09 13:45:30.123] Loading configuration from: rocks-exporter.properties
[2026-02-09 13:45:30.145] Using config file: rocks-exporter.properties
[2026-02-09 13:45:30.146] Orphan limit = 250
[2026-02-09 13:45:30.147] Index prefix = dbidxEntry
[2026-02-09 13:45:30.156] =========== LOADING CABINETS ===========
[2026-02-09 13:45:30.157] Starting cabinet ID scan from: /path/to/cabinet/data
[2026-02-09 13:45:30.189] Found 4 cabinet folders to process
[2026-02-09 13:45:30.190] Processing cabinet folder 1/4: cabinet_folder_1
[2026-02-09 13:45:31.234] Loaded 1523 cabinet IDs in 1077ms
[2026-02-09 13:45:31.235] =========== SCANNING INDEXES ===========
[2026-02-09 13:45:31.236] Starting orphan index scan from: /path/to/index/data
[2026-02-09 13:45:31.267] Found 4 index folders to scan
[2026-02-09 13:45:33.456] Exported 47 orphan indexes in 2221ms
[2026-02-09 13:45:33.457] CSV written → csv_dumps/orphan_indexes.csv
```

## Output

The application generates a CSV file (`orphan_indexes.csv`) with the following columns:
- `type`: Always "index" for orphan index entries
- `name`: The folder/shard name where the orphan was found
- `key`: The decrypted key of the orphan index entry
- `related`: Always "false" for orphan entries
- `cabinet_id`: Empty for orphan entries

## Dependencies

The project includes the following key dependencies (automatically included in the fat JAR):
- **RocksDB JNI (4.8.0)**: Java bindings for RocksDB
- **Apache Thrift (0.9.1)**: For data serialization
- **Apache Commons**: Various utilities
- **SLF4J**: Logging framework
- **Log4j**: Logging implementation

## Troubleshooting

### Maven Dependency Resolution Issues

The most common build issue involves corrupted Maven local repository cache. If you see errors like:
```
Could not transfer artifact org.codehaus.plexus:plexus-utils:jar:3.5.1...
Failed to execute goal org.apache.maven.plugins:maven-resources-plugin...
```

**Solution:**
1. Clear the problematic dependencies from your local Maven cache
2. Force Maven to re-download all dependencies
3. Rebuild the project

```bash
# Clear corrupted cache
rm -rf ~/.m2/repository/org/apache/maven/plugins/maven-resources-plugin
rm -rf ~/.m2/repository/org/codehaus/plexus

# Clean and rebuild with forced updates
mvn clean -U
mvn compile
mvn package
```

### Other Common Issues

1. **File Not Found Errors**: Ensure the paths in your configuration file are correct and accessible
2. **Permission Errors**: Make sure the application has read access to RocksDB files and write access to the output directory
3. **Memory Issues**: For large datasets, consider increasing JVM memory: `java -Xmx4g -jar target/RocksDBHelper-1.0-SNAPSHOT.jar`
4. **Java Version**: Ensure you're using Java 8 or higher: `java -version`

### Debug Mode

For more verbose output, you can modify the logging level in the source code or add system properties.

## Project Structure

```
RocksDBHelper/
├── pom.xml                          # Maven build configuration
├── src/main/java/org/datastealth/
│   ├── RocksDbFinalExporterOneCSVWithPropertiesFile.java  # Main application
│   └── RocksDbFinalExporter_old.java                     # Legacy version
├── src/main/resources/
│   └── rocks-exporter.properties    # Default configuration
├── csv_dumps/                       # Output directory for CSV reports
└── target/                         # Build output directory
    └── RocksDBHelper-1.0-SNAPSHOT.jar  # Fat JAR executable
```

## Contributing

When modifying the code:
1. Ensure all changes compile without errors
2. Test with sample data before processing production databases
3. Update this README if configuration options change
4. Rebuild the fat JAR after any changes: `mvn clean package`

## License

This project is part of the DataStealth organization's internal tooling.
