package org.datastealth.helpers.buffer;


import org.datastealth.helpers.ByteHelper;

import java.nio.charset.StandardCharsets;

public class ByteArrayWriter {
    private byte[] _buffer;

    public ByteArrayWriter() {
        _buffer = new byte[0];
    }

    public ByteArrayWriter writeLong(long value) {
        _buffer = ByteHelper.appendByteArrays(_buffer, ByteHelper.longToBytes(value));
        return this;
    }

    public ByteArrayWriter writeInt(int value) {
        _buffer = ByteHelper.appendByteArrays(_buffer, ByteHelper.intToBytes(value));
        return this;
    }

    public ByteArrayWriter writeShort(short value) {
        _buffer = ByteHelper.appendByteArrays(_buffer, ByteHelper.shortToBytes(value));
        return this;
    }

    public ByteArrayWriter writeByte(byte value) {
        _buffer = ByteHelper.appendByteArrays(_buffer, new byte[]{value});
        return this;
    }

    public ByteArrayWriter writeBytes(byte[] bytesParam) {
        byte[] bytes = bytesParam == null ? new byte[0] : bytesParam;
        writeInt(bytes.length);
        _buffer = ByteHelper.appendByteArrays(_buffer, bytes);
        return this;
    }

    public ByteArrayWriter writeString(String string) {
        if (string == null) {
            writeBytes(new byte[0]);
        } else {
            writeBytes(string.getBytes(StandardCharsets.UTF_8));
        }
        return this;
    }

    public byte[] getBuffer() {
        return _buffer;
    }
}
