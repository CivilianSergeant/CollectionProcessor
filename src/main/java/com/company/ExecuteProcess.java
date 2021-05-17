package com.company;

import com.google.gson.Gson;

import java.sql.*;

public class ExecuteProcess implements Runnable {

    private TabCollection tabCollection;
    private String tabData;
    private String rowId;
    Connection conn;
    private boolean isError=false;
    public ExecuteProcess(Connection c,String i,String tabData){

        this.conn = c;
        this.tabData = tabData;
        this.rowId = i;
    }

    @Override
    public void run() {
        Gson gson = new Gson();
        long start = System.currentTimeMillis();
        this.tabCollection = gson.fromJson(this.tabData,TabCollection.class);

        try {
            if(this.tabCollection.getCollections() != null){
                isError=false;
                System.out.println("Total Item:"+this.tabCollection.getCollections().size());
                conn.setAutoCommit(false);
                this.tabCollection.getCollections().forEach((Collection collection)->{
                    try {
                        String data = collection.getOfficeID()+","+
                                collection.getCenterID()+","+
                                collection.getProductID()+","+
                                collection.getMemberID()+","+
                                collection.getAmount()+",'"+this.tabCollection.getUserId()+"',"+
                                collection.getSid()+","+
                                collection.getTType()+",'"+collection.getToken()+"',"+
                                collection.getPType()+","+
                                collection.getLoanInstallment()+","+
                                collection.getIntInstallment()+","+
                                collection.getIntCharge()+","+
                                collection.getFine();
                        String command = "{call API_Set_CollectionWithDrawal("+data+")}"; //Set_CollectionWithDrawal
                        //String command = "{call SPTestRais('hi',1)}";

                        CallableStatement cstmt = conn.prepareCall(command);
                        boolean executed = cstmt.execute();


                        ResultSet r = cstmt.getResultSet();
                        while (r!=null && r.next()){
                            System.out.println(r.getString(1));
                        }
                        if(executed){
                            System.out.println("GUID:"+collection.getToken()+" "+executed);
                        }

                    } catch (SQLException e) {
                        isError=true;

                        e.printStackTrace();
                    }
                });

                if(isError==false) {
                    Statement stmt = conn.createStatement();
                    stmt.execute("UPDATE TabCollection SET IsProcessed=1 WHERE TabParkingID = " + this.rowId);

                    long finish = System.currentTimeMillis();
                    long timeElapsed = finish - start;
                    System.out.println("Batch Execution Completed by : " + timeElapsed + "ms");

                }else{
                    conn.rollback();
                    System.out.println("Batch Execution Skipped RowID:"+rowId);
                }
                conn.commit();
                conn.setAutoCommit(true);
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
