package com.company;

import com.company.accounts.AccountingProcess;
import com.company.email.EmailService;
import com.company.email.EmailServiceImpl;
import com.company.exception.ExceptionWriter;
import com.company.logger.WriteLog;

import java.io.IOException;
import java.sql.*;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Main {

    static WriteLog writeLog;
    public static void main(String[] args) {
        Timer timer = new Timer();
        long period = Long.valueOf(args[0]);

        if(args.length<6){
            System.out.printf("Please make sure you app instruction has following parameters accordingly");
            System.out.println("java -jar filename period ip dbname dbuser dbpass no-of-thread");
            System.exit(0);
        }

        try {
            Logger logger = Logger.getLogger(args[2]);
            writeLog = new WriteLog(logger);

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        timer.schedule(new MyTask(args[1],
                args[2],
                args[3],
                args[4],
                args[5],
                args[6],writeLog
                ),0,period);
    }
}



class MyTask extends TimerTask{

    String ip;
    String db;
    String user;
    String pass;
    String serviceType;
    int noOfthread;
    WriteLog writeLog;
    MyTask(String ip, String db, String user, String pass,String serviceType, String thNo,WriteLog w){
        this.ip = ip;
        this.db = db;
        this.user = user;
        this.pass = pass;
        this.serviceType = serviceType;
        this.noOfthread = Integer.parseInt(thNo);
        writeLog = w;
    }

    @Override
    public void run() {
        Connection conn = null;
        ExecutorService executor = Executors.newFixedThreadPool(noOfthread);
        String URL = "jdbc:sqlserver://"+this.ip+";databaseName="+db;
        String user = this.user; //live buroapk
        String password = this.pass; //"buroapk@#$2019"; // buroapk@#$2019

        System.out.println("Service Starting ");
        System.out.println("DSN: "+URL);
        System.out.println("User: "+user);
        System.out.println("Password: "+pass);
        System.out.println("No of thread: "+noOfthread);
        EmailService emailService = new EmailServiceImpl();


        try {


            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            conn =  DriverManager.getConnection(URL,user,password);
            if(conn != null){


                long start = System.currentTimeMillis();
                Statement stmt = conn.createStatement();

                String sql="";
                Runnable runnable = null;

                switch (serviceType){
                    case "gbanker":
                        sql="SELECT TOP "+this.noOfthread+" * FROM TabCollection WHERE IsProcessed=0 ORDER BY TabParkingID ASC";
                    break;
                    case "gaccounts":
                    sql="SELECT TOP 20 * FROM AccQueue WHERE IsProcessed=0 ORDER BY QueueId ASC";
                    break;
                }
                ResultSet resultSet = stmt.executeQuery(sql);
                int i = 0;
                while (resultSet.next()) {
                    i++;
                    System.out.println("Has Result");
                    String tabData ="";
                    switch(serviceType){
                        case "gbanker":
                            tabData = resultSet.getString(2);
                            runnable = new ExecuteProcess(conn, resultSet.getString(1), tabData);
                            break;
                        case "gaccounts":
                            tabData = resultSet.getString(4);
                            runnable = new AccountingProcess(conn,resultSet.getString(1),tabData);
                            break;
                    }
                    executor.execute(runnable);
                }
                if(i>0) {
                    System.out.println("Records Found: "+ String.valueOf(i));
                    if(executor.awaitTermination((i+60), TimeUnit.SECONDS)){
                        executor.shutdownNow();
                    }
                }
                long finish = System.currentTimeMillis();
                long timeElapsed = finish - start;
                System.out.println("Elapsed Time: " + timeElapsed + "ms");


                System.out.println("Success");
                resultSet.close();
                conn.close();
                System.out.println("End Process");
            }

        } catch (ClassNotFoundException e) {
            emailService.sendMail(ExceptionWriter.getExceptionMessage(db,ip,e,writeLog));
            e.printStackTrace();
        } catch (SQLException e) {

            emailService.sendMail(ExceptionWriter.getExceptionMessage(db,ip,e,writeLog));
            e.printStackTrace();

        } catch (InterruptedException e) {

            emailService.sendMail(ExceptionWriter.getExceptionMessage(db,ip,e,writeLog));
            e.printStackTrace();
        } finally {
            if(conn!=null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    emailService.sendMail(ExceptionWriter.getExceptionMessage(db,ip,e,writeLog));
                    e.printStackTrace();
                }
                System.out.println("End Session");
            }
        }

    }
}
