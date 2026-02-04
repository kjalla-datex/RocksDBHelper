package org.datastealth;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.rocksdb.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

public class VaultIndexCabinetConsistencyTool2 {

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

        Map<Integer, Set<String>> cabinetFragments =
                loadCabinetFragments(cabinetsRoot);

        validate(indexEntries, cabinetFragments);
    }

    /* ============================================================
       INDEX SCAN
       ============================================================ */

    private static Map<String, IndexEntryInfo> loadIndexEntries(String indexesRoot)
            throws Exception {

        Map<String, IndexEntryInfo> result = new HashMap<>();

        for (File indexNode : Objects.requireNonNull(new File(indexesRoot).listFiles())) {

            File dbPath = new File(indexNode, "rocks/" + VAULT_ID);
            if (!dbPath.exists()) continue;

            try (RocksDB db = RocksDB.openReadOnly(new Options(), dbPath.getPath());
                 RocksIterator it = db.newIterator()) {

                for (it.seekToFirst(); it.isValid(); it.next()) {

                    IndexEntry entry = deserializeIndexEntry(it.value());

                    // Sanity filters
                    if (!VAULT_ID.equals(entry.vaultId)) continue;
                    if (entry.status == IndexEntryStatus.FAILED) continue;

                    String fragmentId = bufferToHex(entry.fragmentId);

                    result.put(fragmentId,
                            new IndexEntryInfo(
                                    fragmentId,
                                    entry.fragmentCount,
                                    entry.storageNodeMap
                            ));
                }
            }
        }
        return result;
    }

    /* ============================================================
       CABINET SCAN
       ============================================================ */

    private static Map<Integer, Set<String>> loadCabinetFragments(String cabinetsRoot)
            throws Exception {

        Map<Integer, Set<String>> result = new HashMap<>();

        for (File cabinetNode : Objects.requireNonNull(new File(cabinetsRoot).listFiles())) {

            // cabinet node name → numeric ID mapping
            Integer cabinetId = cabinetNameToId(cabinetNode.getName());
            if (cabinetId == null) continue;

            File dbPath = new File(cabinetNode, "rocks/" + VAULT_ID);
            if (!dbPath.exists()) continue;

            Set<String> fragments = new HashSet<>();

            try (RocksDB db = RocksDB.openReadOnly(new Options(), dbPath.getPath());
                 RocksIterator it = db.newIterator()) {

                for (it.seekToFirst(); it.isValid(); it.next()) {
                    fragments.add(bytesToHex(it.key()));
                }
            }

            result.put(cabinetId, fragments);
        }
        return result;
    }

    /* ============================================================
       VALIDATION
       ============================================================ */

    private static void validate(
            Map<String, IndexEntryInfo> indexEntries,
            Map<Integer, Set<String>> cabinetFragments) {

        System.out.println("=== INDEX → CABINET VALIDATION ===");

        for (IndexEntryInfo entry : indexEntries.values()) {

            for (Integer cabinetId : entry.storageNodeMap) {

                Set<String> fragments = cabinetFragments.get(cabinetId);

                if (fragments == null || !fragments.contains(entry.fragmentId)) {
                    System.out.printf(
                            "❌ Missing fragment %s on cabinet %d%n",
                            entry.fragmentId, cabinetId
                    );
                }
            }
        }

        System.out.println("\n=== CABINET → INDEX VALIDATION ===");

        for (Map.Entry<Integer, Set<String>> e : cabinetFragments.entrySet()) {
            for (String fragmentId : e.getValue()) {
                if (!indexEntries.containsKey(fragmentId)) {
                    System.out.printf(
                            "⚠️ Orphan fragment %s on cabinet %d%n",
                            fragmentId, e.getKey()
                    );
                }
            }
        }
    }

    /* ============================================================
       Helpers
       ============================================================ */

    private static IndexEntry deserializeIndexEntry(byte[] value) throws Exception {
//        TDeserializer deserializer =
//                new TDeserializer(new TCompactProtocol.Factory());

        TDeserializer deserializer =
                new TDeserializer(new TBinaryProtocol.Factory());
        IndexEntry entry = new IndexEntry();
        deserializer.deserialize(entry, value);
        return entry;
    }

    private static String bufferToHex(ByteBuffer buffer) {
        ByteBuffer dup = buffer.duplicate();
        byte[] bytes = new byte[dup.remaining()];
        dup.get(bytes);
        return bytesToHex(bytes);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes)
            sb.append(String.format("%02x", b));
        return sb.toString();
    }

    /**
     * Map cabinet folder name → storageNodeMap integer
     * YOU must define this mapping
     */
    private static Integer cabinetNameToId(String name) {
        if (name.endsWith("-1")) return 1;
        if (name.endsWith("-2")) return 2;
        if (name.endsWith("-3")) return 3;
        if (name.endsWith("-4")) return 4;
        return null;
    }

    /* ============================================================
       Model
       ============================================================ */

    static class IndexEntryInfo {
        final String fragmentId;
        final int fragmentCount;
        final List<Integer> storageNodeMap;

        IndexEntryInfo(String fragmentId, int fragmentCount, List<Integer> storageNodeMap) {
            this.fragmentId = fragmentId;
            this.fragmentCount = fragmentCount;
            this.storageNodeMap = storageNodeMap;
        }
    }
}
