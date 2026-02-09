package org.datastealth;

import org.rocksdb.*;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RocksDbFinalExporter_old {

    static { RocksDB.loadLibrary(); }

    private static final String DEVICE_UUID =
            "4E85DCEB-5BBF-5BF6-9B92-54EFEBF98724";
    private static final String INDEX_BASE =
            "/Users/kjalla/IdeaProjects/maven/local-server-vault-1/data/storage/index";
//            "/Users/rrashi/Downloads/KavyaRocksDb/storage/index";
    private static final String CABINET_BASE =
            "/Users/kjalla/IdeaProjects/maven/local-server-vault-1/data/storage/cabinet";
    //            "/Users/rrashi/Downloads/KavyaRocksDb/storage/cabinet";
    private static final String OUTPUT_DIR = "csv_dumps";
    // encryption
    private static byte[] ENC_KEY;
    private static byte[] ENC_IV;
    private static final Pattern UUID_PATTERN =
            Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    public static void main(String[] args) throws Exception {
        computeEncryptionKeyIv();
        File outDir = new File(OUTPUT_DIR);
        outDir.mkdirs();
        System.out.println("=========== CABINETS ===========");
        Set<String> cabinetIds = exportCabinets(outDir);
        System.out.println("\n=========== INDEXES ===========");
        exportIndexesAndRelations(outDir, cabinetIds);
        System.out.println("\n Dumped CSV files too → " + outDir.getAbsolutePath());
    }

    private static Set<String> exportCabinets(File outDir) throws Exception {
        Set<String> allCabinetIds = new HashSet<>();
        for (File folder : findFolders(new File(CABINET_BASE))) {
            String name = folder.getName();
            File csv = new File(outDir, "cabinet_" + name + ".csv");
            System.out.println("Exporting cabinet → " + name);
            try (BufferedWriter w = new BufferedWriter(new FileWriter(csv))) {
                w.write("cabinet_id,value_utf8\n");
                for (File shard : findRocksShards(new File(folder, "rocks"))) {
                    try (RocksDB db = RocksDB.openReadOnly(shard.getAbsolutePath());
                         RocksIterator it = db.newIterator()) {
                        for (it.seekToFirst(); it.isValid(); it.next()) {
                            byte[] key = decrypt(it.key());
                            byte[] val = decrypt(it.value());
                            String uuid = bytesToUuidIfPossible(key);
                            // cabinet key IS the UUID (plain text)
                            if (uuid != null) {
                                uuid = uuid.toLowerCase();
                                allCabinetIds.add(uuid);
                                w.write(csv(uuid) + "," +
                                        csv(safeUtf8(val)) + "\n");
                            }
                        }
                    }
                }
            }
        }
        System.out.println("Total cabinet IDs found = " + allCabinetIds.size());
        return allCabinetIds;
    }

    private static void exportIndexesAndRelations(File outDir,
                                                  Set<String> cabinetIds) throws Exception {
        File relFile = new File(outDir, "index_cabinet_relations.csv");
        File orphanFile = new File(outDir, "orphans.csv");
        int orphanCount = 0;

        try (PrintWriter rel = new PrintWriter(relFile);
             PrintWriter orphan = new PrintWriter(orphanFile)) {

            // 🔹 UPDATED HEADER
            rel.println("index_folder,index_key,cabinet_id,exists_in_cabinet");
            orphan.println("index_folder,index_key,missing_cabinet_id,status");

            for (File folder : findFolders(new File(INDEX_BASE))) {
                String name = folder.getName();
                File csv = new File(outDir, "index_" + name + ".csv");
                System.out.println("Exporting index → " + name);

                try (BufferedWriter w = new BufferedWriter(new FileWriter(csv))) {
                    w.write("key_utf8,value_utf8\n");

                    for (File shard : findRocksShards(new File(folder, "rocks"))) {
                        try (RocksDB db = RocksDB.openReadOnly(shard.getAbsolutePath());
                             RocksIterator it = db.newIterator()) {

                            for (it.seekToFirst(); it.isValid(); it.next()) {
                                byte[] k = decrypt(it.key());
                                byte[] v = decrypt(it.value());

                                String keyStr = safeUtf8(k);
                                String valStr = safeUtf8(v);

                                w.write(csv(keyStr) + "," + csv(valStr) + "\n");

                                // 🔹 Extract ANY fragment UUID from index value
                                String fragmentId = extractFirstUuid(valStr);
                                if (fragmentId == null)
                                    continue;

                                boolean exists = cabinetIds.contains(fragmentId);

                                // 🔹 ALWAYS write relation row
                                rel.println(csv(name) + "," +
                                        csv(keyStr) + "," +
                                        csv(fragmentId) + "," +
                                        exists);

                                // 🔹 Orphan handling stays explicit
                                if (!exists) {
                                    orphan.println(csv(name) + "," +
                                            csv(keyStr) + "," +
                                            csv(fragmentId) + ",orphan");
                                    orphanCount++;
                                }
                            }
                        }
                    }
                }
            }
        }

        System.out.println("Relations written → " + relFile.getName());
        System.out.println("Orphans written   → " + orphanFile.getName());
        System.out.println("Total orphans     → " + orphanCount);
    }


    private static String extractFirstUuid(String text) {
        if (text == null) return null;
        Matcher m = UUID_PATTERN.matcher(text);
        if (m.find()) {
            return m.group().toLowerCase();
        }
        return null;
    }


    private static String extractMatchingCabinetUuid(String text,Set<String> cabinetIds){
        if (text == null) return null;
        Matcher m = UUID_PATTERN.matcher(text);
        while (m.find()) {
            String candidate = m.group().toLowerCase();
            if (cabinetIds.contains(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private static byte[] decrypt(byte[] data) {
        if (data == null || data.length % 16 != 0)
            return data;
        try {
            Cipher c = Cipher.getInstance("AES/CBC/PKCS5Padding");
            c.init(Cipher.DECRYPT_MODE,
                    new SecretKeySpec(ENC_KEY, "AES"),
                    new IvParameterSpec(ENC_IV));
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
        if (UUID_PATTERN.matcher(s).matches()){
            return s;
        }
        return null;
    }


    private static String csv(String s) {
        return "\"" + (s == null ? "" : s.replace("\"", "\"\"")) + "\"";
    }
}