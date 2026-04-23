package com.nosql.parser;

public class NasaLogRecord {
    public String host;
    public String timestamp;
    public String logDate;
    public String logHour;
    public String httpMethod;
    public String resourcePath;
    public String protocolVersion;
    public int statusCode;
    public long bytesTransferred;

    @Override
    public String toString() {
        return String.format("[%s] %s %s -> %d (%d bytes)", logDate, httpMethod, resourcePath, statusCode, bytesTransferred);
    }
}