package com.nosql.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser {
    // Regex matches: IP/Host - - [Date] "Request" Status Bytes
    private static final String LOG_PATTERN = "^(\\S+) \\S+ \\S+ \\[(.+?)\\] \"([^\"]*)\" (\\d{3}) (\\d+|-)$";
    private static final Pattern PATTERN = Pattern.compile(LOG_PATTERN);

    private long parsedCount = 0;
    private long malformedCount = 0;

    public NasaLogRecord parseLine(String line) {
        Matcher matcher = PATTERN.matcher(line);

        if (matcher.matches()) {
            NasaLogRecord record = new NasaLogRecord();
            record.host = matcher.group(1);
            record.timestamp = matcher.group(2); // e.g., 01/Jul/1995:00:00:01 -0400

            // Extract log_date and log_hour per project requirements
            if (record.timestamp.length() >= 14) {
                record.logDate = record.timestamp.substring(0, 11); // 01/Jul/1995
                record.logHour = record.timestamp.substring(12, 14); // 00
            }

            // Safely parse the request string (Method Path Protocol)
            String requestStr = matcher.group(3);
            String[] reqParts = requestStr.split("\\s+");
            record.httpMethod = reqParts.length > 0 ? reqParts[0] : "-";
            record.resourcePath = reqParts.length > 1 ? reqParts[1] : "-";
            record.protocolVersion = reqParts.length > 2 ? reqParts[2] : "-";

            // Parse status code
            record.statusCode = Integer.parseInt(matcher.group(4));

            // Parse bytes: convert '-' to 0 per project requirements
            String bytesStr = matcher.group(5);
            record.bytesTransferred = bytesStr.equals("-") ? 0 : Long.parseLong(bytesStr);

            parsedCount++;
            return record;
        } else {
            // Do not silently drop; track the malformed record
            malformedCount++;
            return null;
        }
    }

    public long getParsedCount() { return parsedCount; }
    public long getMalformedCount() { return malformedCount; }
    public void resetCounters() { parsedCount = 0; malformedCount = 0; }
}