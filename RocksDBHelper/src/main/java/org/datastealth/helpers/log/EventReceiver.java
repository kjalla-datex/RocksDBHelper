package org.datastealth.helpers.log;

import java.util.Map;

public interface EventReceiver {
    void processEvent(String level, String message, String className, String methodName, String lineNumber, String threadName, Throwable ex, Map<String, String> tags);
}
