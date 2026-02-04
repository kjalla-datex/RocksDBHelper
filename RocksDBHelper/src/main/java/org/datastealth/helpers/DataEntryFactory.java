package org.datastealth.helpers;

import java.io.IOException;

@FunctionalInterface
public interface DataEntryFactory<T> {
    public T buildDataEntry(byte[] data) throws IOException;
}
