package org.datastealth;

import org.rocksdb.*;


import org.rocksdb.*;

import org.rocksdb.*;

public class RocksDBScanner {
    public static void main(String[] args) {
        // ===== Set your RocksDB path here =====
        String dbPath = "/Users/kjalla/IdeaProjects/maven/local-server-vault-1/data/storage/index/kj-local-index-1/rocks/4E85DCEB-5BBF-5BF6-9B92-54EFEBF98724";
        // ======================================

        RocksDB.loadLibrary();

        // Default options
        Options options = new Options();
        options.setCreateIfMissing(false);

        try {
            // First, list all column families
            java.util.List<byte[]> cfs = RocksDB.listColumnFamilies(options, dbPath);

            java.util.List<ColumnFamilyDescriptor> cfDescriptors = new java.util.ArrayList<>();
            for (byte[] cfName : cfs) {
                cfDescriptors.add(new ColumnFamilyDescriptor(cfName, new ColumnFamilyOptions()));
            }

            // Handles for each column family
            java.util.List<ColumnFamilyHandle> cfHandles = new java.util.ArrayList<>();

            // Open DB with all CFs
            try (DBOptions dbOptions = new DBOptions();
                 RocksDB db = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandles)) {

                for (int i = 0; i < cfHandles.size(); i++) {
                    ColumnFamilyHandle handle = cfHandles.get(i);
                    String cfName = new String(cfDescriptors.get(i).columnFamilyName());

                    System.out.println("\nColumn family: " + cfName);
                    try (RocksIterator it = db.newIterator(handle)) {
                        it.seekToFirst();
                        int count = 0;
                        while (it.isValid() && count < 20) {
                            System.out.println("Key: " + new String(it.key()) + " => Value: " + new String(it.value()));
                            it.next();
                            count++;
                        }
                    }
                }

                // Close column family handles
                for (ColumnFamilyHandle handle : cfHandles) {
                    handle.close();
                }
            }

        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}

