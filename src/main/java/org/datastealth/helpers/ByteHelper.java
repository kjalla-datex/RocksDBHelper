package org.datastealth.helpers;

import org.apache.commons.lang3.ArrayUtils;
import org.datastealth.helpers.log.Log;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ByteHelper {

    private ByteHelper() {
        super();
    }

    public static byte[] longToBytes(long value) {
        byte[] retVal = new byte[8];
        //Pack the location id
        retVal[0] = (byte) ((value >> 56) & 0xff);
        retVal[1] = (byte) ((value >> 48) & 0xff);
        retVal[2] = (byte) ((value >> 40) & 0xff);
        retVal[3] = (byte) ((value >> 32) & 0xff);
        retVal[4] = (byte) ((value >> 24) & 0xff);
        retVal[5] = (byte) ((value >> 16) & 0xff);
        retVal[6] = (byte) ((value >> 8) & 0xff);
        retVal[7] = (byte) ((value) & 0xff);
        return retVal;
    }

    public static long bytesToLong(byte[] data) {
        if (data == null || data.length != 8) {
            throw new IllegalArgumentException("The supplied value must be 8 bytes long in order to convert to a long!");
        }
        return (data[0] & 0xFFL) << 56 | (data[1] & 0xFFL) << 48 |
                (data[2] & 0xFFL) << 40 | (data[3] & 0xFFL) << 32 |
                (data[4] & 0xFFL) << 24 | (data[5] & 0xFFL) << 16 |
                (data[6] & 0xFFL) << 8 | (data[7] & 0xFFL);
    }

    public static byte[] intToBytes(int value) {
        byte[] retVal = new byte[4];
        //Pack the location id
        retVal[0] = (byte) ((value >> 24) & 0xff);
        retVal[1] = (byte) ((value >> 16) & 0xff);
        retVal[2] = (byte) ((value >> 8) & 0xff);
        retVal[3] = (byte) ((value) & 0xff);
        return retVal;
    }

    public static int bytesToint(byte[] data) {
        if (data == null || data.length != 4) {
            throw new IllegalArgumentException("The supplied value must be 4 bytes long in order to convert to a int!");
        }
        return (data[0] & 0xFF) << 24 | (data[1] & 0xFF) << 16 |
                (data[2] & 0xFF) << 8 | (data[3] & 0xFF);
    }

    public static short bytesToshort(byte[] data) {
        if (data == null || data.length != 2) {
            throw new IllegalArgumentException("The supplied value must be 2 bytes long in order to convert to a short!");
        }
        return (short) ((data[0] & 0xFF) << 8 | (data[1] & 0xFF));
    }

    public static byte[] shortToBytes(short value) {
        byte[] retVal = new byte[2];
        retVal[0] = (byte) ((value >> 8) & 0xff);
        retVal[1] = (byte) ((value) & 0xff);
        return retVal;
    }


    /**
     * Note - returns the bytes remaining in the buffer
     *
     * @param data
     * @return any unread but written bytes in the buffer
     */
    public static byte[] byteBufferToArray(ByteBuffer data) {
        if (data == null) {
            return null;
        }
        byte[] retVal = new byte[data.remaining()];
        data.get(retVal, 0, retVal.length);
        return retVal;
    }

    public static ByteBuffer byteArrayToBuffer(byte[] dataParam) {
        byte[] data = dataParam;
        if (data == null) data = new byte[0];
        return ByteBuffer.wrap(data);
    }

    /**
     * Re-formats byte array into a string of bytes with a predetermined width
     * Ex. input is byte array [00 01 02 03 04 05] with width = 3
     * output is a string
     * 00 01 02
     * 03 04 05
     */

    public static String writeBytes(byte[] data, int width) {
        StringBuilder retVal = new StringBuilder();
        int offset = 0;
        for (byte b : data) {
            if (offset > 0) {
                retVal.append(" ");
            }
            String val = Integer.toString(b & 0xFF, 16);
            if (val.length() < 2) {
                val = "0" + val;
            }
            retVal.append(val);
            offset++;
            if (offset >= width) {
                offset = 0;
                retVal.append("\n");
            }
        }
        return retVal.toString();
    }

    public static String writeBytes(byte[] data, int width, int start) {
        StringBuilder retVal = new StringBuilder();
        if (start > data.length) {
            return retVal.toString();
        }
        int offset = 0;
        int index = 0;
        for (byte b : data) {
            index++;
            if (index < start) {
                continue;
            }

            if (offset > 0) {
                retVal.append(" ");
            }
            String val = Integer.toString(b & 0xFF, 16);
            if (val.length() < 2) {
                val = "0" + val;
            }
            retVal.append(val);
            offset++;
            if (offset >= width) {
                offset = 0;
                retVal.append("\n");
            }
        }
        return retVal.toString();
    }

    public static byte[] readBytes(String bytes) {

        List<Integer> byteList = new ArrayList();

        String[] parts = bytes.split("\\W+");
        for(String part: parts) {
            try {
                Integer b = Integer.parseInt(part, 16);
                byteList.add(b);
            }
            catch (NumberFormatException e) {
                Log.debug("Unable to parse integer from string:%s part:%s", bytes, part);
            }
        }
        byte[] returnValue = new byte[byteList.size()];
        for(int i = 0; i < byteList.size(); i++) {
            returnValue[i] = byteList.get(i).byteValue();
        }
        return returnValue;
    }

    public static byte[] appendByteArrays(byte[]... bytes) {
        int totalLength = 0;
        for (byte[] d : bytes) totalLength = totalLength + d.length;
        byte[] retVal = new byte[totalLength];
        int pos = 0;
        for (byte[] d : bytes) {
            System.arraycopy(d, 0, retVal, pos, d.length);
            pos = pos + d.length;
        }
        return retVal;
    }

    public static byte[] concatBytes(byte[] data1, byte[] data2) {
        byte[] retVal = new byte[data1.length + data2.length];
        System.arraycopy(data1, 0, retVal, 0, data1.length);
        System.arraycopy(data2, 0, retVal, data1.length, data2.length);
        return retVal;
    }


    /**
     * Position based
     * <p>
     * http://en.wikipedia.org/wiki/Endianness
     */
    public static short getShort(byte[] data, int pos, boolean littleEndian) {
        if (data.length < pos) throw new IllegalArgumentException("Insufficient bytes supplied");

        if (littleEndian) {
            return (short) ((data[pos + 1] << 8) + (data[pos] & 0xFF));
        } else {
            return (short) ((data[pos] << 8) + (data[pos + 1] & 0xFF));
        }
    }

    public static int getInt(byte[] data, int pos, boolean littleEndian) {
        if (data.length < pos + 4) throw new IllegalArgumentException("Insufficient bytes supplied");
        if (littleEndian) {
            int i1 = (int) data[pos + 3] & 0xFF;
            int i2 = (int) data[pos + 2] & 0xFF;
            int i3 = (int) data[pos + 1] & 0xFF;
            int i4 = (int) data[pos] & 0xFF;
            return (i1 << 24) + (i2 << 16) + (i3 << 8) + i4;
        } else {
            int i1 = (int) data[pos] & 0xFF;
            int i2 = (int) data[pos + 1] & 0xFF;
            int i3 = (int) data[pos + 2] & 0xFF;
            int i4 = (int) data[pos + 3] & 0xFF;
            return (i1 << 24) + (i2 << 16) + (i3 << 8) + i4;
        }
    }

    public static long getLong(byte[] data, int pos, boolean littleEndian) {
        if (data.length < pos + 8) throw new IllegalArgumentException("Insufficient bytes supplied");

        if (littleEndian) {
            long lVal = 0L;
            for (int i = 0; i < 8; i++) {
                long curVal = (long) data[pos + i] & 0xFF;
                curVal = curVal << (i * 8);
                lVal += curVal;
            }
            return lVal;
        } else {
            long lVal = 0L;
            for (int i = 0; i < 8; i++) {
                long curVal = (long) data[pos + i] & 0xFF;
                curVal = curVal << ((7 - i) * 8);
                lVal += curVal;
            }
            return lVal;
        }
    }

    public static String getString(byte[] data, int pos, int maximumLen, boolean unicode) {
        if (!unicode) {
            return getUnicodeString(data, pos, maximumLen);
        } else {
            return getString(data, pos, maximumLen);
        }
    }

    /* NOTE: below code stops at 0x00 byte when doing the String conversion*/
    public static String getString(byte[] data, int pos, int maximumLen) {
        //int maxpos = pos + maximumLen;
        //Because first position starts with 0 we decrement 1 when calculating position from length
        int maxpos = pos + maximumLen - 1;
        int endpos = pos;
        while (data[endpos] != 0x00 && endpos < maxpos) endpos++;
        //the last parameter in String is length. To go from positional index to length we increment by 1
        //if (endpos <= maxpos) return new String(data, pos, endpos - pos);
        if (endpos <= maxpos) return new String(data, pos, endpos - pos + 1);
        return null;
    }


    public static String getUnicodeString(byte[] data, int pos, int maximumLen) {
        //	Check for an empty string
        if (maximumLen == 0) return "";

        //  Search for the trailing null
        int maxpos = pos + (maximumLen * 2);
        int endpos = pos;
        char[] chars = new char[maximumLen];
        int cpos = 0;
        char curChar;

        do {
            //  Get a Unicode character from the buffer
            curChar = (char) (((data[endpos + 1] & 0xFF) << 8) + (data[endpos] & 0xFF));
            //  Add the character to the array
            chars[cpos++] = curChar;
            //  Update the buffer pointer
            endpos += 2;
        } while (curChar != 0 && endpos < maxpos);

        //  Check if we reached the end of the buffer
        if (endpos <= maxpos) {
            if (curChar == 0)
                cpos--;
            return new String(chars, 0, cpos);
        }
        return null;
    }

    public static byte[][] split(byte[] data, char splitChar) {
        byte[][] retVal = new byte[0][];
        int startPos = 0;
        int pos = 0;
        while (pos < data.length) {
            //Matches the split char
            if (data[pos] == splitChar) {
                //chop the part we have
                retVal = ArrayUtils.add(retVal, ArrayUtils.subarray(data, startPos, pos));
                startPos = pos + 1;
            }
            pos++;
        }
        if (startPos < data.length) {
            retVal = ArrayUtils.add(retVal, ArrayUtils.subarray(data, startPos, data.length));
        }

        return retVal;
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    public static String byteArrayToHexString(byte[] bytes) {
        StringBuilder retVal = new StringBuilder();
        for (byte b : bytes) {
            String val = Integer.toString(b & 0xFF, 16);
            if (val.length() < 2) {
                val = "0" + val;
            }
            retVal.append(val);
        }
        return retVal.toString();
    }

    public static boolean startsWith(byte[] array, byte[] prefix) {
        if (array == prefix) {
            return true;
        }
        if (array == null || prefix == null) {
            return false;
        }
        int prefixLength = prefix.length;
        if (prefix.length > array.length) {
            return false;
        }
        for (int i = 0; i < prefixLength; i++) {
            if (array[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
