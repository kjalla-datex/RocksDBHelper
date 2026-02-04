package org.datastealth.helpers.buffer;

import org.apache.commons.lang3.ArrayUtils;
import org.datastealth.helpers.ByteHelper;

import java.nio.charset.StandardCharsets;

public class ByteArrayReader {
    private byte[] _buffer;
    private int _position;

    /**
     * Constructor used to build a buffer for de-serializing
     *
     * @param buffer the backing buffer to read
     */
    public ByteArrayReader(byte[] buffer) {
        _buffer = buffer;
        _position = 0;
    }

    /**
     * reset the position back to 0 to allow re-reading the object
     */
    public void reset() {
        _position = 0;
    }

    public boolean hasMoreBytes() {
        return _position < _buffer.length - 1;
    }

    private byte[] readBufferBytes(int bytesToRead) {
        byte[] retVal = ArrayUtils.subarray(_buffer, _position, _position + bytesToRead);
        _position = _position + bytesToRead;
        return retVal;
    }

    public long readLong() {
        return ByteHelper.bytesToLong(readBufferBytes(8));
    }

    public int readInt() {
        return ByteHelper.bytesToint(readBufferBytes(4));
    }

    public short readShort() {
        return ByteHelper.bytesToshort(readBufferBytes(2));
    }

    public byte readByte() {
        return readBufferBytes(1)[0];
    }

    public byte[] readBytes() {
        return readBufferBytes(readInt());
    }

    public String readString() {
        return new String(readBytes(), StandardCharsets.UTF_8);
    }

    public Boolean readBoolean() {
        return readBufferBytes(1)[0] == 1;
    }

}
