package com.nosql.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser {
    private static final String LOG_PATTERN = "^(\\S+) \\S+ \\S+ \\[(.+?)\\] \"([^\"]*)\" (\\d{3}) (\\d+|-)$";
    private static final Pattern PATTERN = Pattern.compile(LOG_PATTERN);

    private long parsedCount = 0;
    private long malformedCount = 0;

    public NasaLogRecord parseLine(String line) {
        if (line == null || line.trim().isEmpty()) return null;
        
        Matcher matcher = PATTERN.matcher(line);
        if (!matcher.matches()) {
            malformedCount++;
            return null;
        }

        try {
            NasaLogRecord record = new NasaLogRecord();
            record.host = matcher.group(1);
            record.timestamp = matcher.group(2);

            int colonIndex = record.timestamp.indexOf(':');
            if (colonIndex > 0) {
                record.logDate = record.timestamp.substring(0, colonIndex);
                record.logHour = record.timestamp.substring(colonIndex + 1, colonIndex + 3);
            }

            String requestStr = matcher.group(3);
            String[] reqParts = requestStr.split("\\s+");
            record.httpMethod = reqParts.length > 0 ? reqParts[0] : "-";
            record.resourcePath = reqParts.length > 1 ? reqParts[1] : "-";
            record.protocolVersion = reqParts.length > 2 ? reqParts[2] : "-";

            record.statusCode = Integer.parseInt(matcher.group(4));

            String bytesStr = matcher.group(5);
            record.bytesTransferred = (bytesStr.equals("-") || bytesStr.equals("")) ? 0 : Long.parseLong(bytesStr);

            parsedCount++;
            return record;
        } catch (Exception e) {
            malformedCount++;
            return null;
        }
    }

    public long getParsedCount() { return parsedCount; }
    public long getMalformedCount() { return malformedCount; }
    public void resetCounters() { parsedCount = 0; malformedCount = 0; }
}