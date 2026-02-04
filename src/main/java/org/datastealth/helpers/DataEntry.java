package org.datastealth.helpers;


import org.datastealth.helpers.buffer.ByteArrayReader;
import org.datastealth.helpers.buffer.ByteArrayWriter;

import java.io.IOException;
import java.util.Map;

public abstract class DataEntry<T> {
    protected String _documentType;
    protected Long _revision;
    protected T _entry;

    public DataEntry() {
    }

    public DataEntry(String documentType, Long revision, T entry) {
        _documentType = documentType;
        _revision = revision;
        _entry = entry;
    }

    public DataEntry(byte[] data) throws IOException {
        load(data);
    }

    public void load(byte[] data) throws IOException {
        ByteArrayReader reader = new ByteArrayReader(data);
        _documentType = reader.readString();
        _revision = reader.readLong();
        _entry = entryFromBytes(reader.readBytes());
    }

    public String getDocumentType() {
        return _documentType;
    }

    public Long getRevision() {
        return _revision;
    }

    public T getEntry() {
        return _entry;
    }

    public byte[] getBytes() throws IOException {
        ByteArrayWriter writer = new ByteArrayWriter();
        writer.writeString(_documentType);
        writer.writeLong(_revision);
        writer.writeBytes(entryToBytes());
        return writer.getBuffer();
    }

    protected abstract T entryFromBytes(byte[] data) throws IOException;

    protected abstract byte[] entryToBytes() throws IOException;

    public abstract String getPrimaryKey();

    public abstract Map<String, String> getIndexFields();
}
