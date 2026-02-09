package org.datastealth;

import org.rocksdb.*;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.regex.Pattern;

public class RocksDbFinalExporterOneCSVWithPropertiesFile {
//    static { RocksDB.loadLibrary(); }
    // ================= DEFAULTS =================
    private static final int DEFAULT_ORPHAN_LIMIT = 10;
    private static final String DEFAULT_INDEX_PREFIX = "dbidxEntry";
    private static String DEVICE_UUID;
    private static String INDEX_BASE;
    private static String CABINET_BASE;
    private static String OUTPUT_DIR;
    private static int ORPHAN_LIMIT;
    private static String INDEX_PREFIX;
    private static byte[] ENC_KEY;
    private static byte[] ENC_IV;
    private static final Pattern UUID_PATTERN =Pattern.compile("[0-9a-fA-F\\-]{36}");
    private static final Pattern HEX32_PATTERN = Pattern.compile("[0-9a-fA-F]{32}");


    public static void main(String[] args) throws Exception {
        String configPath = args.length>0?args[0]:"/Users/kjalla/IdeaProjects/RocksDBHelper/src/main/resources/rocks-exporter.properties";
        loadConfig(configPath);
        System.out.println("Using config file: " + configPath);
        System.out.println("Orphan limit = " + ORPHAN_LIMIT);
        System.out.println("Index prefix = " + INDEX_PREFIX);
        computeEncryptionKeyIv();
        File outDir = new File(OUTPUT_DIR);
        outDir.mkdirs();
        File unified = new File(outDir, "orphan_indexes.csv");
        try (PrintWriter writer = new PrintWriter(unified)) {
            writer.println("type,name,key,related,cabinet_id");
            System.out.println("=========== LOADING CABINETS ===========");
            Set<String> cabinetIds = loadCabinetIds();
            System.out.println("=========== SCANNING INDEXES ===========");
            int written = exportOrphanIndexes(writer, cabinetIds, ORPHAN_LIMIT);
            System.out.println("Exported orphan indexes = " + written);
        }
        System.out.println("CSV written → " + unified.getAbsolutePath());
    }

    // =========================================================
    // Config loader
    // =========================================================

    private static void loadConfig(String path) throws Exception {
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(path)) {
            props.load(in);
        }
        DEVICE_UUID = require(props, "DEVICE_UUID");
        INDEX_BASE  = require(props, "INDEX_BASE");
        CABINET_BASE = require(props, "CABINET_BASE");
        OUTPUT_DIR  = require(props, "OUTPUT_DIR");
        ORPHAN_LIMIT = Integer.parseInt(
                props.getProperty("DEFAULT_ORPHAN_LIMIT",
                        String.valueOf(DEFAULT_ORPHAN_LIMIT))
        );

        INDEX_PREFIX = props.getProperty("INDEX_PREFIX", DEFAULT_INDEX_PREFIX);
        System.out.println("DEVICE_UUID  = " + DEVICE_UUID);
        System.out.println("INDEX_BASE   = " + INDEX_BASE);
        System.out.println("CABINET_BASE = " + CABINET_BASE);
        System.out.println("OUTPUT_DIR   = " + OUTPUT_DIR);
        System.out.println("ORPHAN_LIMIT = " + ORPHAN_LIMIT);
        System.out.println("INDEX_PREFIX = " + INDEX_PREFIX);
    }

    private static String require(Properties p,String key) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required config: "+key);
        }
        return v.trim();
    }

    // =========================================================
    // Load cabinet IDs
    // =========================================================

    private static Set<String> loadCabinetIds() throws Exception {
        Set<String> allCabinetIds = new HashSet<>();
        for (File folder : findFolders(new File(CABINET_BASE))) {
            for (File shard : findRocksShards(new File(folder, "rocks"))) {
                try (RocksDB db = RocksDB.openReadOnly(shard.getAbsolutePath());
                     RocksIterator it = db.newIterator()) {
                    for (it.seekToFirst(); it.isValid(); it.next()) {
                        byte[] key = decrypt(it.key());
                        String uuid = bytesToUuidIfPossible(key);
                        if (uuid != null) {
                            allCabinetIds.add(uuid.toLowerCase());
                        }
                    }
                }
            }
        }
        return allCabinetIds;
    }

    // Replace bytesToUuidBe to return lower-case
    private static String bytesToUuidBe(byte[] b) {
        if (b == null || b.length != 16) return "";
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(b);
        bb.order(java.nio.ByteOrder.BIG_ENDIAN);
        long msb = bb.getLong();
        long lsb = bb.getLong();
        return new UUID(msb, lsb).toString().toLowerCase();
    }

    // Replace bytesToUuidMixedEndian to return lower-case
    private static String bytesToUuidMixedEndian(byte[] b) {
        if (b == null || b.length != 16) return "";
        byte[] out = new byte[16];
        out[0] = b[3]; out[1] = b[2]; out[2] = b[1]; out[3] = b[0];
        out[4] = b[5]; out[5] = b[4];
        out[6] = b[7]; out[7] = b[6];
        System.arraycopy(b, 8, out, 8, 8);
        return bytesToUuidBe(out).toLowerCase();
    }
//todo add time interval logging progress
    private static Set<String> extractUuidsFromBytes(byte[] bytes) {
        Set<String> uuids = new LinkedHashSet<>();
        if (bytes == null) return uuids;
        for (int i = 0; i + 16 <= bytes.length; i++) {
            byte[] win = Arrays.copyOfRange(bytes, i, i + 16);
            // standard UUID interpretation (big-endian)
            String uuidBe = bytesToUuidBe(win);
            if (UUID_PATTERN.matcher(uuidBe).matches()) uuids.add(uuidBe);
            // mixed-endian / GUID interpretation (first three fields little-endian)
            String uuidMixed = bytesToUuidMixedEndian(win);
            if (UUID_PATTERN.matcher(uuidMixed).matches()) uuids.add(uuidMixed);
            // also check hex-no-dash representation in-place
            String hex = bytesToHex(win);
            java.util.regex.Matcher m32 = HEX32_PATTERN.matcher(hex);
            if (m32.find()) {
                String dashed = hex.substring(0,8) + "-" + hex.substring(8,12) + "-" + hex.substring(12,16) + "-" + hex.substring(16,20) + "-" + hex.substring(20,32);
                uuids.add(dashed.toLowerCase());
            }
        }
        return uuids;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    // Replace extractUuidsFromString
    private static Set<String> extractUuidsFromString(String s) {
        Set<String> uuids = new LinkedHashSet<>();
        if (s == null) return uuids;
        java.util.regex.Matcher matcher = UUID_PATTERN.matcher(s);
        while (matcher.find()) uuids.add(matcher.group().toLowerCase());
        // look for 32-hex (no dashes) and convert to dashed form (lower-case)
        java.util.regex.Matcher m32 = HEX32_PATTERN.matcher(s);
        while (m32.find()) {
            String h = m32.group();
            String dashed = h.substring(0,8) + "-" + h.substring(8,12) + "-" + h.substring(12,16) + "-" + h.substring(16,20) + "-" + h.substring(20,32);
            uuids.add(dashed.toLowerCase());
        }
        return uuids;
    }

    // =========================================================
    // Orphan index detection (dbidxEntry only)
    // =========================================================

    private static int exportOrphanIndexes(PrintWriter writer,
                                           Set<String> cabinetIds,
                                           int limit) throws Exception {
        int orphanCount = 0;
        for (File folder : findFolders(new File(INDEX_BASE))) {
            String name = folder.getName();
            for (File shard : findRocksShards(new File(folder, "rocks"))) {
                try (RocksDB db = RocksDB.openReadOnly(shard.getAbsolutePath());
                     RocksIterator it = db.newIterator()) {
                    for (it.seekToFirst(); it.isValid(); it.next()) {
                        byte[] keyBytes = it.key();
                        byte[] valBytes = it.value();
                        String keyStr = safeUtf8(decrypt(keyBytes));
                        if (!keyStr.contains(INDEX_PREFIX)) continue;
                        byte[] decryptedVal = decrypt(valBytes);
                        Set<String> uuids = extractUuidsFromBytes(decryptedVal);
                        uuids.addAll(extractUuidsFromString(safeUtf8(decryptedVal)));
                        boolean related = uuids.stream().anyMatch(u -> cabinetIds.contains(u.toLowerCase()));
                        if (!related) {
                            writer.println(
                                    csv("index") + "," +
                                            csv(name) + "," +
                                            csv(keyStr) + "," +
                                            csv("false") + "," +
                                            csv("")
                            );
                            orphanCount++;
                            if (orphanCount >= limit) {
                                System.out.println("Reached orphan limit. Stopping scan.");
                                return orphanCount;
                            }
                        }
                    }
                }
            }
        }
        return orphanCount;
    }

    private static byte[] decrypt(byte[] data) {
        if (data == null || data.length % 16 != 0)
            return data;
        try {
            Cipher c = Cipher.getInstance("AES/CBC/PKCS5Padding");
            c.init(Cipher.DECRYPT_MODE,new SecretKeySpec(ENC_KEY, "AES"),new IvParameterSpec(ENC_IV));
            return c.doFinal(data);
        } catch (Exception e) {
            return data;
        }
    }

    private static void computeEncryptionKeyIv() throws Exception {
        MessageDigest sha = MessageDigest.getInstance("SHA-384");
        byte[] hash = sha.digest(DEVICE_UUID.getBytes(StandardCharsets.UTF_8));
        ENC_KEY = Arrays.copyOfRange(hash, 0, 32);
        ENC_IV = Arrays.copyOfRange(hash, hash.length - 16, hash.length);
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

    private static String safeUtf8(byte[] b) {
        if (b == null) return "";
        return new String(b, StandardCharsets.UTF_8).replace("\0", "");
    }

    private static String bytesToUuidIfPossible(byte[] b) {
        if (b == null) return null;
        String s = safeUtf8(b);
        return UUID_PATTERN.matcher(s).matches() ? s : null;
    }

    private static String csv(String s) {
        return "\"" + (s == null ? "" : s.replace("\"", "\"\"")) + "\"";
    }
}
