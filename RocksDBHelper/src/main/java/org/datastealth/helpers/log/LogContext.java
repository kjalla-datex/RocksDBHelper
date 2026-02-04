package org.datastealth.helpers.log;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LogContext {

    public static final LogContext EMPTY;

    static {
        LogContext temp = new LogContext();
        temp._isDefault = true;
        EMPTY = temp;
    }

    private boolean _isDefault = false;
    private Map<String, String> _tags = new HashMap<>();
    private LogContext _parent = null;

    public boolean isDefault() {
        return _isDefault;
    }

    public LogContext() {
        super();
    }

    public LogContext(Map<String, String> tags) {
        _tags.putAll(tags);
    }

    public LogContext(LogContext parent, Map<String, String> tags) {
        if (tags!=null && !tags.isEmpty()) {
            _tags.putAll(tags);
        }
        if (parent != null) {
            _tags.putAll(parent.tags());
        }
        _parent = parent;
    }

    public void initWithParent(LogContext parent) {
        if (parent != null) {
            _tags.putAll(parent.tags());
            _parent = parent;
        }
    }

    public void addTag(String name, String value) {
        _tags.put(name, value);
    }

    public Map<String, String> tags() {
        return Collections.unmodifiableMap(_tags);
    }

    public void info(String message, Object... params) {
        Log.log("INFO", _tags != null ? _tags : _parent.tags(), message, params);
    }

    public void debug(String message, Object... params) {
        Log.log("DEBUG", _tags != null ? _tags : _parent.tags(), message, params);
    }

    public void warn(String message, Object... params) {
        Log.log("WARN", _tags != null ? _tags : _parent.tags(), message, params);
    }

    public void error(String message, Object... params) {
        Log.log("ERROR", _tags != null ? _tags : _parent.tags(), message, params);
    }

    public void trace(String message, Object... params) {
        Log.log("TRACE", _tags != null ? _tags : _parent.tags(), message, params);
    }

    public void log(String logLevel, String message, Object... params) {
        Log.log(logLevel, _tags != null ? _tags : _parent.tags(), message, params);
    }

    public void log(Log.LogLocation logLocation, String logLevel, String message, Object... params) {
        Log.writeLogWithLocation(logLocation, logLevel, _tags != null ? _tags : _parent.tags(), message, params);

    }
}