package org.datastealth;

import org.rocksdb.*;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class RocksDbViewer {

    public static void main(String[] args) throws RocksDBException {

        String dbPath = "/Users/kjalla/IdeaProjects/maven/local-server-vault-1/data/storage/index/kj-local-index-1/rocks/4E85DCEB-5BBF-5BF6-9B92-54EFEBF98724";
        try (final Options options = new Options().setCreateIfMissing(false);
             final RocksDB db = RocksDB.openReadOnly(options, dbPath)) {
            try (RocksIterator iter = db.newIterator()) {
                for (iter.seekToFirst(); iter.isValid(); iter.next()) {
//                    String key = Base64.getEncoder().encodeToString(iter.key());
                    String key = new String(iter.key(), StandardCharsets.UTF_8);
//                    String value = new String(iter.value(), StandardCharsets.UTF_8);
                    String value = Base64.getEncoder().encodeToString(iter.value());
                    System.out.println("key: " + key + " -> value: " + value);
                }
            }
        } catch (RocksDBException e) {
            System.err.println("RocksDB Exception: " + e.getMessage());
        }
        System.out.println("CABINETS----------------------------------CABINETS----------------------------------CABINETS----------------------------------");
        String dbPath2 = "/Users/kjalla/IdeaProjects/maven/local-server-vault-1/data/storage/cabinet/hpsf-kj-local-1/rocks/4E85DCEB-5BBF-5BF6-9B92-54EFEBF98724";
        try (final Options options = new Options().setCreateIfMissing(false);
             final RocksDB db2 = RocksDB.openReadOnly(options, dbPath2)) {
            try (RocksIterator iter = db2.newIterator()) {
                for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                    String key = Base64.getEncoder().encodeToString(iter.key());
//                    String key = new String(iter.key(), StandardCharsets.UTF_8);
//                    String value = new String(iter.value(), StandardCharsets.UTF_8);
                    String value = Base64.getEncoder().encodeToString(iter.value());
                    System.out.println("key: " + key + " -> value: " + value);
                }
            }
        } catch (RocksDBException e) {
            System.err.println("RocksDB Exception: " + e.getMessage());
        }
    }

    // Pick the CF with the most keys (likely the data CF)
    private static ColumnFamilyHandle selectLargestCF(RocksDB db, List<ColumnFamilyHandle> cfHandles) {
        long maxKeys = -1;
        ColumnFamilyHandle largest = cfHandles.get(0);
        for (ColumnFamilyHandle handle : cfHandles) {
            long count = 0;
            try (RocksIterator it = db.newIterator(handle)) {
                it.seekToFirst();
                while (it.isValid()) {
                    count++;
                    it.next();
                }
            }
            if (count > maxKeys) {
                maxKeys = count;
                largest = handle;
            }
        }
        return largest;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02X", b));
        return sb.toString();
    }
}