package com.spbsu.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * User: svasilinets
 * Date: 28.04.12
 * Time: 19:13
 */
class LogRecord implements Writable {
    private String userAgent;
    private long timestamp;
    private String ip;
    private String request;
    private String referPage;

    public LogRecord(){}

    private LogRecord(String userAgent, String request, String time, String ip, String page) throws ParseException {
        this.userAgent = userAgent;
        this.ip = ip;
        this.referPage = page;
        this.request = request;
        SimpleDateFormat sformat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss z");
        timestamp = sformat.parse(time).getTime();
    }

    public String getIp() {
        return ip;
    }

    public String getRequest() {
        return request;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getReferPage() {
        return referPage;
    }




    static int[] nextPart(String line, int start) {
        int end;

        while (line.charAt(start) == ' ') {
            start++;
        }
        if (line.charAt(start) == '"') {
            start++;
            end = start;
            while (line.charAt(end) != '"') {
                end++;
            }
        } else {
            end = start;
            while (end < line.length() && line.charAt(end) != ' ') {
                end++;
            }
        }

        return new int[]{start, end};
    }


    static LogRecord parseLogString(String line) throws ParseException {
        int ind = line.indexOf(' ');
        String ip = line.substring(0, ind);
        int openBr = line.indexOf('[');
        int closeBr = line.indexOf(']');
        String date = line.substring(openBr + 1, closeBr);
        line = line.substring(closeBr + 2);

        int endof = line.indexOf("\"");
        line = line.substring(endof);
        int[] pos = nextPart(line, 0);
        String request = line.substring(pos[0], pos[1]);
        pos = nextPart(line, pos[1] + 1);
        String requestCode = line.substring(pos[0], pos[1]);
        pos = nextPart(line, pos[1] + 1);
        String bytes = line.substring(pos[0], pos[1]);
        pos = nextPart(line, pos[1] + 1);
        String page = line.substring(pos[0], pos[1]);
        pos = nextPart(line, pos[1] + 1);
        String userAgent = line.substring(pos[0], pos[1]);
        return new LogRecord(userAgent, request, date, ip, page);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(userAgent);
        dataOutput.writeLong(timestamp);
        dataOutput.writeUTF(ip);
        dataOutput.writeUTF(referPage);
        dataOutput.writeUTF(request);

    }

    @Override
    public void readFields(DataInput input) throws IOException {

        userAgent = input.readUTF();
        timestamp = input.readLong();
        ip = input.readUTF();
        referPage = input.readUTF();
        request = input.readUTF();
    }
}
