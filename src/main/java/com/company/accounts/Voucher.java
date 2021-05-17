package com.company.accounts;

import java.util.List;

public class Voucher {

    private long trx_master_id;
    private int office_id;
    private String trx_date;
    private String voucher_no;
    private String voucher_type;
    private long user_id;
    private String create_date;
    private List<VoucherDetail> details;

    public Voucher(){}

    public Voucher(int trx_master_id, int office_id, String trx_date, String voucher_no,
                   String voucher_type, int user_id, String create_date,
                   List<VoucherDetail> details) {
        this.trx_master_id = trx_master_id;
        this.office_id = office_id;
        this.trx_date = trx_date;
        this.voucher_no = voucher_no;
        this.voucher_type = voucher_type;
        this.user_id = user_id;
        this.create_date = create_date;
        this.details = details;
    }

    public long getTrx_master_id() {
        return trx_master_id;
    }

    public void setTrx_master_id(long trx_master_id) {
        this.trx_master_id = trx_master_id;
    }

    public int getOffice_id() {
        return office_id;
    }

    public void setOffice_id(int office_id) {
        this.office_id = office_id;
    }

    public String getTrx_date() {
        return trx_date;
    }

    public void setTrx_date(String trx_date) {
        this.trx_date = trx_date;
    }

    public String getVoucher_no() {
        return voucher_no;
    }

    public void setVoucher_no(String voucher_no) {
        this.voucher_no = voucher_no;
    }

    public String getVoucher_type() {
        return voucher_type;
    }

    public void setVoucher_type(String voucher_type) {
        this.voucher_type = voucher_type;
    }

    public long getUser_id() {
        return user_id;
    }

    public void setUser_id(long user_id) {
        this.user_id = user_id;
    }

    public String getCreate_date() {
        return create_date;
    }

    public void setCreate_date(String create_date) {
        this.create_date = create_date;
    }

    public List<VoucherDetail> getDetails() {
        return details;
    }

    public void setDetails(List<VoucherDetail> details) {
        this.details = details;
    }
}

