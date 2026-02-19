package org.datastealth;

import org.rocksdb.*;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RocksDbIndexUuidCounterHuge {

    private static String INDEX_BASE;
    private static String OUTPUT_DIR;
    private static String INDEX_PREFIX;

    private static final Pattern UUID_PATTERN =
            Pattern.compile("[0-9a-fA-F\\-]{36}");

    private static final SimpleDateFormat LOG_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private static final int CHUNK_SIZE = 500_000;

    // Logging and threshold
    private static final int LOG_INTERVAL = 10_000;
    private static long DEFAULT_ORPHAN_LIMIT; // Change if needed

    // ================= CRYPTO CACHE =================
    private static class Crypto {
        final byte[] key;
        final byte[] iv;
        Crypto(byte[] key, byte[] iv) { this.key = key; this.iv = iv; }
    }
    private static final Map<String,Crypto> CRYPTO_CACHE = new HashMap<>();

    public static void main(String[] args) throws Exception {

        RocksDB.loadLibrary();

        String configPath = args.length>0
                ? args[0]
                : "/Users/rrashi/IdeaProjects/gitMaven/dss/rocks-exporter.properties";

        loadConfig(configPath);

        File outDir = new File(OUTPUT_DIR);
        outDir.mkdirs();

        File rawFile = new File(outDir, "uuids_raw.tmp");

        log("PHASE 1: Extract UUIDs with indexName");
        extractAllUuids(rawFile);

        log("PHASE 2: Create sorted chunks");
        List<File> chunks = createSortedChunks(rawFile);

        log("PHASE 3: Merge + Count");
        File finalCsv = new File(outDir, "index_uuid_counts.csv");
        mergeAndCount(chunks, finalCsv);

        rawFile.delete();
        for (File f : chunks) f.delete();

        log("DONE → " + finalCsv.getAbsolutePath());
    }

    // ================= PHASE 1 =================
    private static void extractAllUuids(File output) throws Exception {

        List<File> folders = findFolders(new File(INDEX_BASE));
        int totalIndexes = folders.size();
        log("Total indexes to process: " + totalIndexes);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {

            long totalRecords = 0;
            int processedIndexes = 0;

            long startTime = System.currentTimeMillis();
            long lastLogTime = startTime;
            long lastLogCount = 0;

            for (File folder : folders) {

                processedIndexes++;
                String indexName = folder.getName();

                List<File> shards = findRocksShards(new File(folder, "rocks"));
                Map<File, Crypto> shardCrypto = new HashMap<>();

                for (File shard : shards) {
                    String deviceUuid = shard.getName();
                    shardCrypto.put(shard, getCrypto(deviceUuid));
                }

                log(String.format("Processing index %d/%d: %s",
                        processedIndexes, totalIndexes, indexName));


                // ---------- STEP 2: Actual extraction with logging ----------
                long indexEntryCount = 0;

                for (File shard : shards) {

                    Crypto crypto = shardCrypto.get(shard);

                    try (RocksDB db = RocksDB.openReadOnly(shard.getAbsolutePath());
                         RocksIterator it = db.newIterator()) {

                        for (it.seekToFirst(); it.isValid(); it.next()) {

                            byte[] keyBytes = it.key();
                            String keyStr = safeUtf8(decrypt(keyBytes, crypto));

                            if (!keyStr.contains(INDEX_PREFIX)) continue;

                            String uuid = extractUuid(keyStr);
                            if (uuid == null) continue;

                            writer.write(indexName + "|" + uuid.toLowerCase());
                            writer.newLine();

                            totalRecords++;
                            indexEntryCount++;

                            // ---------- LOGGING ----------
                            if (totalRecords % LOG_INTERVAL == 0) {
                                long now = System.currentTimeMillis();
                                long batchTimeMs = now - lastLogTime;
                                long totalTimeMs = now - startTime;
                                long batchCount = totalRecords - lastLogCount;

                                double batchRate = (batchCount * 1000.0) / batchTimeMs;
                                double avgRate = (totalRecords * 1000.0) / totalTimeMs;
                                long remaining = DEFAULT_ORPHAN_LIMIT - totalRecords;
                                double etaSec = avgRate > 0 ? remaining / avgRate : 0;

                                Runtime rt = Runtime.getRuntime();
                                long usedMem = (rt.totalMemory() - rt.freeMemory())/1024/1024;
                                long freeMem = rt.freeMemory()/1024/1024;

                                log(String.format(
                                        "Processed records: %,d | Last %d: %.2f sec | Batch rate: %,.0f rec/sec | Avg rate: %,.0f rec/sec | ETA: %.2f sec | Index %d/%d | Index entries so far: %,d | Mem used=%dMB free=%dMB",
                                        totalRecords,
                                        LOG_INTERVAL,
                                        batchTimeMs / 1000.0,
                                        batchRate,
                                        avgRate,
                                        etaSec,
                                        processedIndexes,
                                        totalIndexes,
                                        indexEntryCount,
                                        usedMem,
                                        freeMem
                                ));

                                lastLogTime = now;
                                lastLogCount = totalRecords;
                            }

                            // ---------- THRESHOLD CHECK ----------
                            if (totalRecords >= DEFAULT_ORPHAN_LIMIT) {
                                log("Reached threshold of " + DEFAULT_ORPHAN_LIMIT + " records. Stopping extraction.");
                                return;
                            }

                        } // end iterator
                    } // end db
                } // end shard loop

                log(String.format("Finished index %d/%d: %s | Total extracted entries: %,d",
                        processedIndexes, totalIndexes, indexName, indexEntryCount));

            } // end folder loop
        } // end writer
    }

    // ================= PHASE 2 =================
    private static List<File> createSortedChunks(File rawFile) throws Exception {
        List<File> chunks = new ArrayList<>();
        List<String> buffer = new ArrayList<>(CHUNK_SIZE);

        try (BufferedReader reader = new BufferedReader(new FileReader(rawFile))) {
            String line;
            int chunkIndex = 0;
            while ((line = reader.readLine()) != null) {
                buffer.add(line);
                if (buffer.size() >= CHUNK_SIZE) {
                    chunks.add(writeChunk(buffer, chunkIndex++));
                    buffer.clear();
                }
            }
            if (!buffer.isEmpty()) chunks.add(writeChunk(buffer, chunkIndex));
        }

        log("Created " + chunks.size() + " sorted chunks");
        return chunks;
    }

    private static File writeChunk(List<String> buffer, int index) throws Exception {
        Collections.sort(buffer);
        File chunk = new File(OUTPUT_DIR, "chunk_" + index + ".tmp");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(chunk))) {
            for (String s : buffer) {
                writer.write(s);
                writer.newLine();
            }
        }
        return chunk;
    }

    // ================= PHASE 3 =================
    private static void mergeAndCount(List<File> chunks, File output) throws Exception {
        PriorityQueue<ChunkReader> pq =
                new PriorityQueue<>(Comparator.comparing(cr -> cr.current));

        for (File chunk : chunks) {
            ChunkReader cr = new ChunkReader(chunk);
            if (cr.current != null) pq.add(cr);
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {

            writer.write("indexName,value,count");
            writer.newLine();

            String prev = null;
            long count = 0;

            while (!pq.isEmpty()) {

                ChunkReader cr = pq.poll();
                String val = cr.current;

                if (!val.equals(prev)) {
                    if (prev != null) writeResult(writer, prev, count);
                    prev = val;
                    count = 1;
                } else {
                    count++;
                }

                if (cr.next()) {
                    pq.add(cr);
                } else {
                    cr.close();
                }
            }

            if (prev != null) writeResult(writer, prev, count);
        }
    }

    private static void writeResult(BufferedWriter writer, String combined, long count) throws Exception {
        int sep = combined.indexOf('|');
        String indexName = combined.substring(0, sep);
        String uuid = combined.substring(sep + 1);
        writer.write(indexName + "," + uuid + "," + count);
        writer.newLine();
    }

    // ================= CHUNK READER =================
    private static class ChunkReader {
        BufferedReader reader;
        String current;
        ChunkReader(File file) throws Exception {
            reader = new BufferedReader(new FileReader(file));
            current = reader.readLine();
        }
        boolean next() throws Exception {
            current = reader.readLine();
            return current != null;
        }
        void close() throws Exception { reader.close(); }
    }

    // ================= CRYPTO =================
    private static Crypto getCrypto(String deviceUuid) throws Exception {
        Crypto c = CRYPTO_CACHE.get(deviceUuid);
        if (c != null) return c;

        MessageDigest sha = MessageDigest.getInstance("SHA-384");
        byte[] hash = sha.digest(deviceUuid.getBytes(StandardCharsets.UTF_8));

        c = new Crypto(
                Arrays.copyOfRange(hash, 0, 32),
                Arrays.copyOfRange(hash, hash.length - 16, hash.length)
        );

        CRYPTO_CACHE.put(deviceUuid, c);
        return c;
    }

    private static byte[] decrypt(byte[] data, Crypto crypto) {
        if (data == null || data.length % 16 != 0) return data;
        try {
            Cipher c = Cipher.getInstance("AES/CBC/PKCS5Padding");
            c.init(Cipher.DECRYPT_MODE,
                    new SecretKeySpec(crypto.key, "AES"),
                    new IvParameterSpec(crypto.iv));
            return c.doFinal(data);
        } catch (Exception e) {
            return data;
        }
    }

    private static String safeUtf8(byte[] b) {
        if (b == null) return "";
        int len = 0;
        for (byte value : b) if (value != 0) len++;
        byte[] cleaned = new byte[len];
        int j = 0;
        for (byte value : b) if (value != 0) cleaned[j++] = value;
        return new String(cleaned, StandardCharsets.UTF_8);
    }

    // ================= HELPERS =================
    private static String extractUuid(String key) {
        Matcher m = UUID_PATTERN.matcher(key);
        return m.find() ? m.group() : null;
    }

    private static void loadConfig(String path) throws Exception {
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(path)) { props.load(in); }

        INDEX_BASE = props.getProperty("INDEX_BASE").trim();
        OUTPUT_DIR = props.getProperty("OUTPUT_DIR").trim();
        INDEX_PREFIX = props.getProperty("INDEX_PREFIX", "dbidxEntry");
        DEFAULT_ORPHAN_LIMIT = Long.parseLong(props.getProperty("DEFAULT_ORPHAN_LIMIT", "205000000"));
    }

    private static List<File> findFolders(File root) {
        List<File> out = new ArrayList<>();
        if (!root.exists()) return out;
        for (File f : Objects.requireNonNull(root.listFiles())) {
            if (new File(f, "rocks").exists()) out.add(f);
            else if (f.isDirectory()) out.addAll(findFolders(f));
        }
        return out;
    }

    private static List<File> findRocksShards(File rocks) {
        List<File> out = new ArrayList<>();
        if (!rocks.exists()) return out;
        for (File f : Objects.requireNonNull(rocks.listFiles())) {
            if (new File(f, "CURRENT").exists()) out.add(f);
        }
        return out;
    }

    private static void log(String message) {
        System.out.println("[" + LOG_FORMAT.format(new Date()) + "] " + message);
    }
}
