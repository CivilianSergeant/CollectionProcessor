package com.company.exception;

import com.company.logger.WriteLog;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionWriter {

    public static String getExceptionMessage(String db, String ip, Exception e, WriteLog writeLog){



        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        String msg = "Database: "+db+"\r\n";
        msg += "IP: "+ip+"\r\n";
        msg += "Issue: "+ sw.toString();
        writeLog.writeToFile(msg);

        return msg;
    }
}
