package org.datastealth;

import org.rocksdb.*;
import java.io.File;
import java.util.*;

public class VaultStorageConsistencyTool {

    static {
        RocksDB.loadLibrary();
    }

    private static final String VAULT_ID =
            "4E85DCEB-5BBF-5BF6-9B92-54EFEBF98724";

    public static void main(String[] args) throws Exception {
        String cabinetsRoot = "/Users/kjalla/IdeaProjects/maven/local-server-vault-1/data/storage/cabinet";
        String indexesRoot  = "/Users/kjalla/IdeaProjects/maven/local-server-vault-1/data/storage/index";

        Map<String, IndexEntryInfo> indexEntries =
                loadIndexEntries(indexesRoot);

        Map<String, Set<String>> cabinetFragments =
                loadCabinetFragments(cabinetsRoot);

        validate(indexEntries, cabinetFragments);
    }

    /* ============================================================
       INDEX SCAN
       ============================================================ */

    private static Map<String, IndexEntryInfo> loadIndexEntries(String indexesRoot)
            throws Exception {

        Map<String, IndexEntryInfo> result = new HashMap<>();

        for (File indexNode : new File(indexesRoot).listFiles()) {
            File dbPath = new File(indexNode, "rocks/" + VAULT_ID);
            if (!dbPath.exists()) continue;

            try (RocksDB db = RocksDB.openReadOnly(new Options(), dbPath.getPath());
                 RocksIterator it = db.newIterator()) {

                for (it.seekToFirst(); it.isValid(); it.next()) {
                    IndexEntry entry = IndexEntry.deserialize(it.value());

                    String fragmentId = bytesToHex(entry.getFragmentId());

                    result.put(fragmentId,
                            new IndexEntryInfo(
                                    fragmentId,
                                    entry.getFragmentCount(),
                                    entry.getCommittedNodes()
                            ));
                }
            }
        }
        return result;
    }

    /* ============================================================
       CABINET SCAN
       ============================================================ */

    private static Map<String, Set<String>> loadCabinetFragments(String cabinetsRoot)
            throws Exception {

        Map<String, Set<String>> result = new HashMap<>();

        for (File cabinetNode : new File(cabinetsRoot).listFiles()) {
            File dbPath = new File(cabinetNode, "rocks/" + VAULT_ID);
            if (!dbPath.exists()) continue;

            Set<String> fragments = new HashSet<>();

            try (RocksDB db = RocksDB.openReadOnly(new Options(), dbPath.getPath());
                 RocksIterator it = db.newIterator()) {

                for (it.seekToFirst(); it.isValid(); it.next()) {
                    fragments.add(bytesToHex(it.key()));
                }
            }

            result.put(cabinetNode.getName(), fragments);
        }
        return result;
    }

    /* ============================================================
       VALIDATION
       ============================================================ */

    private static void validate(
            Map<String, IndexEntryInfo> indexEntries,
            Map<String, Set<String>> cabinetFragments) {

        System.out.println("=== INDEX → CABINET CHECK ===");

        for (IndexEntryInfo entry : indexEntries.values()) {
            for (String cabinetNode : entry.committedNodes) {

                Set<String> fragments = cabinetFragments.get(cabinetNode);
                if (fragments == null || !fragments.contains(entry.fragmentId)) {
                    System.out.printf(
                            "❌ Missing fragment %s on cabinet %s%n",
                            entry.fragmentId, cabinetNode
                    );
                }
            }
        }

        System.out.println("\n=== CABINET → INDEX CHECK ===");

        for (Map.Entry<String, Set<String>> e : cabinetFragments.entrySet()) {
            for (String fragmentId : e.getValue()) {
                if (!indexEntries.containsKey(fragmentId)) {
                    System.out.printf(
                            "⚠️ Orphan fragment %s on cabinet %s%n",
                            fragmentId, e.getKey()
                    );
                }
            }
        }
    }

    /* ============================================================
       Helpers / Models
       ============================================================ */

    static class IndexEntryInfo {
        final String fragmentId;
        final int fragmentCount;
        final List<String> committedNodes;

        IndexEntryInfo(String id, int count, List<String> nodes) {
            this.fragmentId = id;
            this.fragmentCount = count;
            this.committedNodes = nodes;
        }
    }

    static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes)
            sb.append(String.format("%02x", b));
        return sb.toString();
    }

    /* ------------------------------------------------------------
       Replace with your real IndexEntry
       ------------------------------------------------------------ */
    static class IndexEntry {
        byte[] fragmentId;
        short fragmentCount;
        List<String> committedNodes;

        static IndexEntry deserialize(byte[] value) {
            throw new UnsupportedOperationException("Implement real deserialization");
        }

        byte[] getFragmentId() { return fragmentId; }
        short getFragmentCount() { return fragmentCount; }
        List<String> getCommittedNodes() { return committedNodes; }
    }
}
