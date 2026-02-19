package org.datastealth;

import org.rocksdb.*;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RocksDbFinalExporterOneCSVWithPropertiesFile {

    // ================= DEFAULTS =================
    private static final int DEFAULT_ORPHAN_LIMIT = 10;
    private static final String DEFAULT_INDEX_PREFIX = "dbidxEntry";

    private static String INDEX_BASE;
    private static String CABINET_BASE;
    private static String OUTPUT_DIR;
    private static int ORPHAN_LIMIT;
    private static String INDEX_PREFIX;

    private static final Pattern UUID_PATTERN = Pattern.compile("[0-9a-fA-F\\-]{36}");
    private static final Pattern HEX32_PATTERN = Pattern.compile("[0-9a-fA-F]{32}");
    private static final SimpleDateFormat LOG_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    // ================= CRYPTO CACHE =================
    private static class Crypto {
        final byte[] key;
        final byte[] iv;
        Crypto(byte[] key, byte[] iv) { this.key = key; this.iv = iv; }
    }
    private static final Map<String, Crypto> CRYPTO_CACHE = new HashMap<>();

    // ================= MAIN =================
    public static void main(String[] args) throws Exception {
        String configPath = args.length>0?args[0]:"/Users/rrashi/IdeaProjects/gitMaven/dss/rocks-exporter.properties";
        loadConfig(configPath);

        log("Orphan limit = " + ORPHAN_LIMIT);
        log("Index prefix = " + INDEX_PREFIX);

        File outDir = new File(OUTPUT_DIR);
        outDir.mkdirs();
        File csvFile = new File(outDir, "orphan_indexes.csv");

        try (PrintWriter writer = new PrintWriter(csvFile)) {
            writer.println("type,name,key,related,cabinet_id");

            log("=========== LOADING CABINETS ===========");
            long startTime = System.currentTimeMillis();
            Set<String> cabinetIds = scanCabinets();
            log("Loaded " + cabinetIds.size() + " cabinet IDs in " + (System.currentTimeMillis() - startTime) + "ms");

            log("=========== SCANNING INDEXES ===========");
            startTime = System.currentTimeMillis();
            int written = scanIndexes(writer, cabinetIds, ORPHAN_LIMIT);
            log("Exported " + written + " orphan indexes in " + (System.currentTimeMillis() - startTime) + "ms");
        }

        log("CSV written → " + csvFile.getAbsolutePath());
    }

    // ================= CONFIG =================
    private static void loadConfig(String path) throws Exception {
        log("Loading configuration from: " + path);
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(path)) {
            props.load(in);
        }

        INDEX_BASE = require(props, "INDEX_BASE");
        CABINET_BASE = require(props, "CABINET_BASE");
        OUTPUT_DIR = require(props, "OUTPUT_DIR");

        ORPHAN_LIMIT = Integer.parseInt(props.getProperty("DEFAULT_ORPHAN_LIMIT", String.valueOf(DEFAULT_ORPHAN_LIMIT)));
        INDEX_PREFIX = props.getProperty("INDEX_PREFIX", DEFAULT_INDEX_PREFIX);

        log("Configuration loaded successfully");
    }

    private static String require(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) throw new IllegalArgumentException("Missing required config: " + key);
        return v.trim();
    }

    // ================= CABINET SCAN =================
    private static Set<String> scanCabinets() throws Exception {
        Set<String> allCabinetIds = new HashSet<>();
        List<File> folders = findFolders(new File(CABINET_BASE));
        log("Found " + folders.size() + " cabinet folders to process");

        int folderCount = 0;
        long totalKeys = 0;

        for (File folder : folders) {
            folderCount++;
            long folderKeys = 0;
            log("Processing cabinet folder " + folderCount + "/" + folders.size() + ": " + folder.getName());

            for (File shard : findRocksShards(new File(folder, "rocks"))) {
                String deviceUuid = shard.getName();
                Crypto crypto = getCrypto(deviceUuid);

                try (RocksDB db = RocksDB.openReadOnly(shard.getAbsolutePath());
                     RocksIterator it = db.newIterator()) {
                    for (it.seekToFirst(); it.isValid(); it.next()) {
                        byte[] key = decrypt(it.key(), crypto);
                        String uuid = bytesToUuidIfPossible(key);
                        if (uuid != null) {
                            allCabinetIds.add(uuid.toLowerCase());
                            folderKeys++;
                            totalKeys++;
                        }
                    }
                }
            }

            log("Folder '" + folder.getName() + "' processed: " + folderKeys + " keys");
        }

        log("Cabinet loading complete: " + allCabinetIds.size() + " unique cabinet IDs from " + totalKeys + " total keys");
        return allCabinetIds;
    }

    // ================= INDEX SCAN =================
    private static int scanIndexes(PrintWriter writer, Set<String> cabinetIds, int limit) throws Exception {
        List<File> folders = findFolders(new File(INDEX_BASE));
        log("Found " + folders.size() + " index folders to scan");

        int orphanCount = 0;
        long totalKeys = 0;
        long indexKeys = 0;

        for (File folder : folders) {
            log("Scanning index folder: " + folder.getName());
            long folderKeys = 0;
            long folderIndexKeys = 0;
            long folderOrphans = 0;

            for (File shard : findRocksShards(new File(folder, "rocks"))) {
                // Correct DEVICE_UUID for this shard
                String deviceUuid = shard.getName();
                Crypto crypto = getCrypto(deviceUuid);

                try (RocksDB db = RocksDB.openReadOnly(shard.getAbsolutePath());
                     RocksIterator it = db.newIterator()) {
                    for (it.seekToFirst(); it.isValid(); it.next()) {
                        folderKeys++;
                        totalKeys++;

                        byte[] keyBytes = it.key();
                        byte[] valBytes = it.value();
                        String keyStr = safeUtf8(decrypt(keyBytes, crypto));

                        if (!keyStr.contains(INDEX_PREFIX)) continue;

                        folderIndexKeys++;
                        indexKeys++;

                        byte[] decryptedVal = decrypt(valBytes, crypto);
                        Set<String> uuids = extractUuidsFromBytes(decryptedVal);
                        uuids.addAll(extractUuidsFromString(safeUtf8(decryptedVal)));

                        boolean related = uuids.stream().anyMatch(u -> cabinetIds.contains(u.toLowerCase()));

                        if (!related) {
                            writer.println(csv("index") + "," + csv(folder.getName()) + "," + csv(keyStr)
                                    + "," + csv("false") + "," + csv(""));
                            orphanCount++;
                            folderOrphans++;

                            if (orphanCount >= limit) {
                                log("Reached orphan limit (" + limit + "). Stopping scan.");
                                return orphanCount;
                            }
                        }

                        if (totalKeys % 10000 == 0) {
                            log("Progress: " + totalKeys + " keys processed, " + indexKeys + " index keys, " + orphanCount + " orphans");
                        }
                    }
                }
            }

            log("Folder '" + folder.getName() + "' complete: " + folderKeys + " keys (" + folderIndexKeys
                    + " index keys), " + folderOrphans + " orphans found");
        }

        log("Index scan complete: processed " + totalKeys + " total keys, " + indexKeys + " index keys, found " + orphanCount + " orphans");
        return orphanCount;
    }

    // ================= DECRYPTION =================
    private static Crypto getCrypto(String deviceUuid) throws Exception {
        Crypto c = CRYPTO_CACHE.get(deviceUuid);
        if (c != null) return c;

        MessageDigest sha = MessageDigest.getInstance("SHA-384");
        byte[] hash = sha.digest(deviceUuid.getBytes(StandardCharsets.UTF_8));
        c = new Crypto(Arrays.copyOfRange(hash, 0, 32), Arrays.copyOfRange(hash, hash.length - 16, hash.length));
        CRYPTO_CACHE.put(deviceUuid, c);
        return c;
    }

    private static byte[] decrypt(byte[] data, Crypto crypto) {
        if (data == null || data.length % 16 != 0) return data;
        try {
            Cipher c = Cipher.getInstance("AES/CBC/PKCS5Padding");
            c.init(Cipher.DECRYPT_MODE, new SecretKeySpec(crypto.key, "AES"), new IvParameterSpec(crypto.iv));
            return c.doFinal(data);
        } catch (Exception e) {
            return data;
        }
    }

    // ================= UTILITIES =================
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

    private static String safeUtf8(byte[] b) {
        if (b == null) return "";
        int len = 0;
        for (byte value : b) if (value != 0) len++;
        byte[] cleaned = new byte[len];
        int j = 0;
        for (byte value : b) if (value != 0) cleaned[j++] = value;
        return new String(cleaned, StandardCharsets.UTF_8);
    }

    private static String bytesToUuidIfPossible(byte[] b) {
        if (b == null) return null;
        String s = safeUtf8(b);
        return UUID_PATTERN.matcher(s).matches() ? s : null;
    }

    private static Set<String> extractUuidsFromBytes(byte[] bytes) {
        Set<String> uuids = new LinkedHashSet<>();
        if (bytes == null) return uuids;
        for (int i = 0; i + 16 <= bytes.length; i++) {
            byte[] win = Arrays.copyOfRange(bytes, i, i + 16);
            String uuidBe = bytesToUuidBe(win);
            if (UUID_PATTERN.matcher(uuidBe).matches()) uuids.add(uuidBe);
            String uuidMixed = bytesToUuidMixedEndian(win);
            if (UUID_PATTERN.matcher(uuidMixed).matches()) uuids.add(uuidMixed);
            String hex = bytesToHex(win);
            if (HEX32_PATTERN.matcher(hex).find()) {
                String dashed = hex.substring(0,8) + "-" + hex.substring(8,12) + "-" + hex.substring(12,16)
                        + "-" + hex.substring(16,20) + "-" + hex.substring(20,32);
                uuids.add(dashed.toLowerCase());
            }
        }
        return uuids;
    }

    private static Set<String> extractUuidsFromString(String s) {
        Set<String> uuids = new LinkedHashSet<>();
        if (s == null) return uuids;
        Matcher matcher = UUID_PATTERN.matcher(s);
        while (matcher.find()) uuids.add(matcher.group().toLowerCase());
        Matcher m32 = HEX32_PATTERN.matcher(s);
        while (m32.find()) {
            String h = m32.group();
            String dashed = h.substring(0,8) + "-" + h.substring(8,12) + "-" + h.substring(12,16)
                    + "-" + h.substring(16,20) + "-" + h.substring(20,32);
            uuids.add(dashed.toLowerCase());
        }
        return uuids;
    }

    private static String bytesToUuidBe(byte[] b) {
        if (b == null || b.length != 16) return "";
        ByteBuffer bb = java.nio.ByteBuffer.wrap(b).order(java.nio.ByteOrder.BIG_ENDIAN);
        long msb = bb.getLong();
        long lsb = bb.getLong();
        return new java.util.UUID(msb, lsb).toString().toLowerCase();
    }

    private static String bytesToUuidMixedEndian(byte[] b) {
        if (b == null || b.length != 16) return "";
        byte[] out = new byte[16];
        out[0]=b[3]; out[1]=b[2]; out[2]=b[1]; out[3]=b[0];
        out[4]=b[5]; out[5]=b[4]; out[6]=b[7]; out[7]=b[6];
        System.arraycopy(b,8,out,8,8);
        return bytesToUuidBe(out).toLowerCase();
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b & 0xff));
        return sb.toString();
    }

    private static String csv(String s) {
        return "\"" + (s == null ? "" : s.replace("\"","\"\"")) + "\"";
    }

    private static void log(String message) {
        System.out.println("[" + LOG_FORMAT.format(new Date()) + "] " + message);
    }
}