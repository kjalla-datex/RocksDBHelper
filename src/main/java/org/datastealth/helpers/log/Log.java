package org.datastealth.helpers.log;

import org.datastealth.helpers.StringHelper;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Log {

    public static final String LEVEL_DEBUG = "DEBUG";
    public static final String LEVEL_INFO = "INFO";
    public static final String LEVEL_WARN = "WARN";
    public static final String LEVEL_ERROR = "ERROR";
    public static final String LEVEL = "LEVEL";

    private static final Log _instance = new Log();
    private static final Map<String, LogReceiver> _logReceivers = new HashMap<>();
    private static LogLevelSupplier _logLevelSupplier = level -> true;
    private static LocationInfoSupplier _locationInfoSupplier = () -> false;
    private static EventReceiver _eventReceiver;

    //Capture Log4J Data
    static {
        Logger.getRootLogger().getLoggerRepository().resetConfiguration();
        Appender appender = new AppenderSkeleton() {
            @Override
            protected void append(LoggingEvent event) {
                Throwable ex = event.getThrowableInformation() != null ? event.getThrowableInformation().getThrowable() : null;
                writeLogWithLocation(event.getLevel().toString(), event.getRenderedMessage(),
                        event.getLocationInformation().getClassName(),
                        event.getLocationInformation().getMethodName(),
                        event.getLocationInformation().getLineNumber(),
                        ex);
            }

            @Override
            public void close() {
                //nothing required
            }

            @Override
            public boolean requiresLayout() {
                return false;
            }
        };
        Logger.getRootLogger().addAppender(appender);
        Logger.getRootLogger().setLevel(_instance._debug ? Level.DEBUG : Level.INFO); //todo set properly
    }

    private boolean _debug = false;

    private Log() {
    }

    public static LogReceiver getLogReceiver(String name) {
        return _logReceivers.get(name);
    }

    public static void addLogReceiver(String name, LogReceiver logReceiver) {
        _logReceivers.put(name, logReceiver);
    }

    public static void setLogLevelSupplier(LogLevelSupplier supplier) {
        _logLevelSupplier = supplier;
    }

    public static void setLocationInfoSupplier(LocationInfoSupplier locationInfoSupplier) {
        _locationInfoSupplier = locationInfoSupplier;
    }

    public static void setEventReceiver(EventReceiver eventReceiver) {
        _eventReceiver = eventReceiver;
    }

    public static void enableDebug() {
        _instance._debug = true;
    }

    public static void disableDebug() {
        _instance._debug = false;
    }

    public static boolean isDebug() {
        return _instance._debug;
    }

    public static void setDebug(boolean debug) {
        _instance._debug = debug;
    }

    public static LogTagWrapper withTags(Map<String, String> tags) {
        return new LogTagWrapper(tags);
    }

    public static void debug(String message, Object... params) {
        log(LEVEL_DEBUG, message, params);
    }

    public static void info(String message, Object... params) {
        log(LEVEL_INFO, message, params);
    }

    public static void warn(String message, Object... params) {
        log(LEVEL_WARN, message, params);
    }

    public static void error(String message, Object... params) {
        log(LEVEL_ERROR, message, params);
    }

    public static void trap(String trapType, String message, Object... params) {
        Map<String, String> tags = new HashMap<>();
        tags.put("trapType", trapType);
        log("TRAP", tags, message, params);
    }

    public static void log(String level, String message, Object... args) {
        writeLog(level, message, args);
    }

    public static void log(String level, Map<String, String> tags, String message, Object... args) {
        writeLog(level, tags, message, args);
    }

    public static void writeLog(String level, String message, Object... args) {
        writeLog(level, null, message, args);
    }

    public static void writeLog(String level, Map<String, String> tags, String message, Object... args) {
        //If the log manager isn't handling this level then just go ahead and exit
        if (_logLevelSupplier != null && !_logLevelSupplier.isLogging(level)) return;

        String className = "";
        String methodName = "";
        String lineNumber = "";
        boolean logInfoLocation = _locationInfoSupplier == null || _locationInfoSupplier.isLogInfoLocation();

        //See if we should be logging the location information.  By default INFO/DEBUG do NOT unless enabled in the LogManager
        if (logInfoLocation || !(LEVEL_DEBUG.equalsIgnoreCase(level) || LEVEL_INFO.equalsIgnoreCase(level))) {
            //Generate a stack trace to get the calling Class
            //Use new Exception().getStackTrace() for performance reasons.  See: http://bugs.java.com/bugdatabase/view_bug.do?bug_id=6375302
            StackTraceElement[] stackTrace = new Exception().getStackTrace();
            int pos = 1;
            StackTraceElement ste = stackTrace[pos];
            className = ste.getClassName();
            while (className.startsWith(Log.class.getName()) && pos < stackTrace.length) {
                ste = stackTrace[pos++];
                className = ste.getClassName();
            }
            methodName = ste.getMethodName();
            lineNumber = Integer.toString(ste.getLineNumber());
        }
        writeLogWithLocation(level, message, className, methodName, lineNumber, tags, args);
    }

    public static void writeLogWithLocation(LogLocation logLocation, String level, String message, Object... args) {
        writeLogWithLocation(logLocation, level, null, message, args);
    }

    public static void writeLogWithLocation(LogLocation logLocation, String level, Map<String, String> tags, String message, Object... args) {
        LogLocation targetLogLocation = logLocation;
        if (targetLogLocation == null) {
            targetLogLocation = getLogLocation(level);
        }
        writeLogWithLocation(level, message, targetLogLocation.getClassName(), targetLogLocation.getMethodName(), targetLogLocation.getLineNumber(), tags, args);
    }

    public static void writeLogWithLocation(String level, String message, String className, String methodName, String lineNumber, Object... args) {
        writeLogWithLocation(level, message, className, methodName, lineNumber, null, args);
    }

    @SuppressWarnings({"squid:S1166", "squid:S106"}) // Format Message Exception - Error suppressed
    public static void writeLogWithLocation(String level, String message, String className, String methodName, String lineNumber, Map<String, String> tags, Object... args) {

        String threadName = Thread.currentThread().getName();
        String formattedMessage;
        try {
            formattedMessage = String.format(message, args);
        } catch (Throwable e) {
            formattedMessage = "ERROR[" + message + "," + ArrayUtils.toString(args) + "]";
        }

        Throwable ex = null;
        for (Object o : args) {
            if (o instanceof Throwable) {
                ex = (Throwable) o;
                break;
            }
        }

        if (!_logReceivers.isEmpty()) {
            for (Map.Entry<String, LogReceiver> lr : _logReceivers.entrySet()) {
                try {
                    lr.getValue().log(level, formattedMessage, className, methodName, lineNumber, threadName, ex, tags);
                } catch (Throwable e) {
                    System.out.printf("[%s] Unable to send log message to [%s] [%s:%s:%s] [%tF %<tT.%<tL] %s%n", level, lr.getKey(), threadName, className, lineNumber, new Date(), formattedMessage);
                }
            }
        } else {
            if (!_instance._debug && LEVEL_DEBUG.equals(level)) {
                return;
            }
            System.out.printf("[%s] [%s:%s:%s] [%tF %<tT.%<tL] %s%n", level, threadName, className, lineNumber, new Date(), formattedMessage);
            if (_eventReceiver != null) {
                _eventReceiver.processEvent(level, formattedMessage, className, methodName, lineNumber, threadName, ex, tags);
            }
        }
    }

    public static LogLocation getLogLocation(String logLevel) {
        return getLogLocation(1, Log.class.getName(), logLevel);
    }

    public static LogLocation getLogLocation(int startLevel, String prefix, String logLevel) {
        int pos = 1;

        if (startLevel >= 0 && startLevel <= 10) {
            pos = startLevel;
        }

        String prefixMatcher = Log.class.getName();
        if (StringHelper.isNotBlank(prefix)) {
            prefixMatcher = prefix;
        }

        LogLocation returnValue = new LogLocation();
        boolean logInfoLocation = _locationInfoSupplier == null || _locationInfoSupplier.isLogInfoLocation();

        if (!logInfoLocation) {
            return returnValue;
        }

        StackTraceElement[] stackTrace = new Exception().getStackTrace();
        if (pos >= stackTrace.length) {
            pos = stackTrace.length - 1;
        }
        StackTraceElement ste = stackTrace[pos];
        returnValue.setClassName(ste.getClassName());
        while (returnValue.getClassName().startsWith(prefixMatcher) && pos < stackTrace.length) {
            ste = stackTrace[pos++];
            returnValue.setClassName(ste.getClassName());
        }
        returnValue.setMethodName(ste.getMethodName());
        returnValue.setLineNumber(Integer.toString(ste.getLineNumber()));

        return returnValue;
    }


    public static class LogTagWrapper {
        private final Map<String, String> _tags = new HashMap<>();

        public LogTagWrapper(Map<String, String> tags) {
            _tags.putAll(tags);
        }

        public LogTagWrapper addTags(Map<String, String> tags) {
            _tags.putAll(tags);
            return this;
        }

        public LogTagWrapper addTag(String name, String value) {
            _tags.put(name, value);
            return this;
        }

        public void debug(String message, Object... params) {
            Log.log(LEVEL_DEBUG, _tags, message, params);
        }

        public void info(String message, Object... params) {
            Log.log(LEVEL_INFO, _tags, message, params);
        }

        public void warn(String message, Object... params) {
            Log.log(LEVEL_WARN, _tags, message, params);
        }

        public void error(String message, Object... params) {
            Log.log(LEVEL_ERROR, _tags, message, params);
        }

        public void log(String level, String message, Object... args) {
            Log.log(level, _tags, message, args);
        }
    }

    public static class LogLocation {

        private String _className = "";
        private String _methodName = "";
        private String _lineNumber = "";

        public String getClassName() {
            return _className;
        }

        public void setClassName(String className) {
            this._className = className;
        }

        public String getMethodName() {
            return _methodName;
        }

        public void setMethodName(String methodName) {
            this._methodName = methodName;
        }

        public String getLineNumber() {
            return _lineNumber;
        }

        public void setLineNumber(String lineNumber) {
            this._lineNumber = lineNumber;
        }
    }
}
