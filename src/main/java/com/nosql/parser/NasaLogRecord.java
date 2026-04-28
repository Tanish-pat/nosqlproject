package com.nosql.parser;

public class NasaLogRecord {
    public String host = "-";
    public String timestamp = "-";
    public String logDate = "-";
    public String logHour = "00";
    public String httpMethod = "-";
    public String resourcePath = "-";
    public String protocolVersion = "-";
    public int statusCode = 0;
    public long bytesTransferred = 0;

    @Override
    public String toString() {
        return String.format("[%s %s] %s %s %s -> %d (%d bytes)",
                logDate, logHour, httpMethod, resourcePath, protocolVersion, statusCode, bytesTransferred);
    }
}