package com.company.logger;


import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class WriteLog {

    private Logger logger;
    private FileHandler fh;

    public WriteLog(Logger logger) throws IOException {
        this.logger = logger;
        fh = new FileHandler("/home/buro/service-apps/logs/error.log",true);
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);

    }

    public void writeToFile(String msg) {
        logger.info(msg);
    }

}
