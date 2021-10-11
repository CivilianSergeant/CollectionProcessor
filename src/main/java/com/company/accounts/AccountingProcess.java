package com.company.accounts;

import com.google.gson.Gson;

import java.sql.*;
import java.text.ParseException;
import java.util.List;

public class AccountingProcess implements Runnable {

    private AccQueue accQueue;
    private String tabData;
    private String rowId;
    Connection conn;
    private boolean isError=false;

    public AccountingProcess(Connection c,String i,String tabData){
        this.conn = c;
        this.tabData = tabData;
        this.rowId = i;
    }

    @Override
    public void run() {
        Gson gson = new Gson();
        long start = System.currentTimeMillis();
        this.accQueue = gson.fromJson(this.tabData,AccQueue.class);

        try{
            conn.setAutoCommit(false);
            List<Voucher> vouchers = this.accQueue.getVouchers();

            List<ChartAccount> chartAccounts = this.accQueue.getChartAccounts();

            if(chartAccounts.size()>0){
                for(ChartAccount chartAccount : chartAccounts){
                    updateChartAccount(chartAccount);
                }
            }

            if(vouchers.size()>0) {
                for (Voucher voucher : vouchers) {
                    String voucherNo = voucher.getVoucher_no();
                    long userId = voucher.getUser_id();

                    // find existing voucher by voucherNo and userID

                    Voucher _voucher = getVoucher(voucherNo, userId);
                    if (_voucher.getTrx_master_id()!=0) {
                        // update voucher and details
                        List<VoucherDetail> voucherDetails = voucher.getDetails();
                        for (VoucherDetail voucherDetail : voucherDetails) {

                            int updated = updateVoucherDetail(_voucher,voucherDetail);
                            if(updated<=0){
                                insertVoucherDetail(_voucher.getTrx_master_id(),voucherDetail);

                            }
                        }

                    } else {
                        // create voucher and details
                        long voucherId = insertVoucher(voucher);

                        if (voucherId > 0) {
                            List<VoucherDetail> voucherDetails = voucher.getDetails();
                            for (VoucherDetail voucherDetail : voucherDetails) {

                                insertVoucherDetail(voucherId,voucherDetail);
                            }

                        }
                    }
                }
            }

            Statement stmt = conn.createStatement();
            stmt.execute("UPDATE AccQueue SET IsProcessed=1 WHERE QueueId = " + this.rowId);

            long finish = System.currentTimeMillis();
            long timeElapsed = finish - start;
            System.out.println("Batch Execution Completed by : " + timeElapsed + "ms");

            conn.commit();
            conn.setAutoCommit(true);
        }catch(Exception ex){
            try {
                conn.rollback();
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            isError=true;

            ex.printStackTrace();
        }


    }

    public Voucher getVoucher(String voucherNo, long userId) throws SQLException {
        String sql = "SELECT * FROM AccTrxMaster WHERE VoucherNo=? AND OfficeID=?";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setString(1,voucherNo);
        pstmt.setLong(2,userId);
        ResultSet resultSet = pstmt.executeQuery();

        Voucher voucher = new Voucher();
        if(resultSet.next()) {
            voucher.setTrx_master_id(resultSet.getLong("TrxMasterID"));
            voucher.setOffice_id(resultSet.getInt("OfficeID"));
            voucher.setVoucher_no(resultSet.getString("VoucherNo"));
            voucher.setVoucher_type(resultSet.getString("VoucherType"));
            voucher.setUser_id(Long.valueOf(resultSet.getString("CreateUser")));
            voucher.setCreate_date(resultSet.getString("CreateDate"));
        }
        return voucher;
    }

    public long insertVoucher(Voucher voucher) throws SQLException, ParseException {
//        conn.setAutoCommit(false);
        String insertSql = "INSERT INTO AccTrxMaster (OfficeID,TrxDate,VoucherNo,VoucherType,VoucherTypeApp,IsPosted,OrgID,CreateUser,CreateDate)" +
                " VALUES (?,'"+voucher.getTrx_date()+"',?,?,?,?,?,?,'"+voucher.getCreate_date()+"')";

//        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
//        java.util.Date voucherDate  =  df.parse(voucher.getTrx_date());
//        java.util.Date createDate = df.parse(voucher.getCreate_date());

//        Instant vIt = Instant.parse(voucher.getTrx_date()+"Z");
//        Instant vCIt = Instant.parse(voucher.getCreate_date()+"Z");
//
//        ZoneId zoneId = ZoneId.of ( "Asia/Dhaka" );
//        ZonedDateTime zdt = ZonedDateTime.ofInstant ( vIt , zoneId );
//        ZonedDateTime zCdt = ZonedDateTime.ofInstant ( vCIt , zoneId );
//        LocalDate voucherLocalDate = zdt.toLocalDate();
//        LocalDate voucherCreateLocalDate = zCdt.toLocalDate();

        PreparedStatement insertStmt = conn.prepareStatement(insertSql,Statement.RETURN_GENERATED_KEYS);
        insertStmt.setInt(1,voucher.getOffice_id());
//        insertStmt.setDate(2, Date.valueOf(voucherLocalDate));
        insertStmt.setString(2,voucher.getVoucher_no());
        insertStmt.setString(3,(voucher.getVoucher_type()=="bank")? "BC": "CA");
        insertStmt.setString(4,voucher.getVoucher_type());
        insertStmt.setBoolean(5,true);
        insertStmt.setInt(6,voucher.getOffice_id());
        insertStmt.setString(7,String.valueOf(voucher.getUser_id()));
//        insertStmt.setDate(9,Date.valueOf(voucherCreateLocalDate));

       int affectedRows = insertStmt.executeUpdate();
//        conn.commit();
        long inserted = -1;
//        if(generatedKeys.next()){
//            inserted = generatedKeys.getLong(0);
//        }
        if(affectedRows>0) {
            try (ResultSet generatedKeys = insertStmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    inserted = generatedKeys.getLong(1);
                } else {
                    throw new SQLException("Creating user failed, no ID obtained.");
                }
            }
        }

        return inserted;
    }

    public int updateVoucherDetail(Voucher voucher, VoucherDetail voucherDetail) throws SQLException{
        String updateSql = "UPDATE AccTrxDetail SET Credit=?, Debit=?,Narration=? WHERE TrxMasterID=? AND AccID=?";
        PreparedStatement updateStmt = conn.prepareStatement(updateSql);
        updateStmt.setDouble(1,voucherDetail.getCredit());
        updateStmt.setDouble(2,voucherDetail.getDebit());
        updateStmt.setString(3,voucherDetail.getNarration());
        updateStmt.setLong(4,voucher.getTrx_master_id());
        updateStmt.setInt(5,voucherDetail.getAcc_id());

        return updateStmt.executeUpdate();
    }

    public int updateChartAccount(ChartAccount chartAccount) throws SQLException{
        String updateSql = "UPDATE AccChart SET AccName=? WHERE AccID=?";
        PreparedStatement updateStmt = conn.prepareStatement(updateSql);
        updateStmt.setString(1,chartAccount.getAcc_name());
        updateStmt.setInt(2,chartAccount.getAcc_id());

        return updateStmt.executeUpdate();
    }

    public long insertVoucherDetail(long voucherId,VoucherDetail voucherDetail) throws SQLException, ParseException {
        String insertSql = "INSERT INTO AccTrxDetail (TrxMasterID,AccID,Credit,Debit,Narration,CreateUser,CreateDate)" +
                " VALUES (?,?,?,?,?,?,'"+voucherDetail.getCreate_date()+"')";


        PreparedStatement insertStmt = conn.prepareStatement(insertSql,Statement.RETURN_GENERATED_KEYS);
        insertStmt.setLong(1,voucherId);
        insertStmt.setInt(2,voucherDetail.getAcc_id());
        insertStmt.setDouble(3,voucherDetail.getCredit());
        insertStmt.setDouble(4,voucherDetail.getDebit());
        insertStmt.setString(5,voucherDetail.getNarration());
        insertStmt.setString(6,String.valueOf(voucherDetail.getUser_id()));


        int affectedRows = insertStmt.executeUpdate();

        long inserted = -1;
        if(affectedRows>0) {
            try (ResultSet generatedKeys = insertStmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    inserted = generatedKeys.getLong(1);
                } else {
                    throw new SQLException("Creating user failed, no ID obtained.");
                }
            }
        }

        return inserted;
    }
}
