package org.datastealth.helpers.dss;


import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.datastealth.IndexEntry;
import org.datastealth.helpers.DataEntry;
import org.datastealth.helpers.DataEntryFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexEntryRow extends DataEntry<IndexEntry> {
    public static final String DOCUMENT_TYPE = "idxEntry";
    public static final String INDEX_TOKEN_ID = "token";
    public static final String INDEX_ALT_ID = "altId";

    public IndexEntryRow(IndexEntry entry) {
        super(DOCUMENT_TYPE,
                entry.getUpdateTimestamp() > 0 ? entry.getUpdateTimestamp() : entry.getTimestamp(),
                entry);
    }

    public IndexEntryRow(byte[] data) throws IOException {
        super(data);
    }

    @Override
    protected IndexEntry entryFromBytes(byte[] bytes) throws IOException {
        TDeserializer ds = new TDeserializer();
        IndexEntry retVal = new IndexEntry();
        try {
            ds.deserialize(retVal, bytes);
        } catch (TException e) {
            throw new IOException("Error de-serializing data!", e);
        }
        return retVal;
    }

    @Override
    protected byte[] entryToBytes() throws IOException {
        try {
            TSerializer serializer = new TSerializer();
            return serializer.serialize(getEntry());
        } catch (TException e) {
            throw new IOException("Unable to serialize object", e);
        }
    }

    public static String buildPrimaryKey(String vaultId, String uniqueId) {
        if (uniqueId == null) return vaultId + "/";
        return vaultId + "/" + uniqueId;
    }


    @Override
    public String getPrimaryKey() {
        return buildPrimaryKey(getEntry().getVaultId(), getEntry().getUniqueId());
    }

    @Override
    public Map<String, String> getIndexFields() {
        Map<String, String> retVal = new HashMap<>();
        if (getEntry().isSetToken())
            retVal.put(INDEX_TOKEN_ID, getEntry().getVaultId() + "/" + getEntry().getToken());
        if (getEntry().isSetAlternateId())
            retVal.put(INDEX_ALT_ID, getEntry().getVaultId() + "/" + getEntry().getAlternateId());
        if (getEntry().isSetMetaData()) {
            getEntry().getMetaData()
                    .forEach((name, val) -> retVal.put(getEntry().getVaultId() + "/" + name, val));
        }
        return retVal;
    }

    public static class Factory implements DataEntryFactory<IndexEntryRow> {
        @Override
        public IndexEntryRow buildDataEntry(byte[] data) throws IOException {
            return new IndexEntryRow(data);
        }
    }
}
