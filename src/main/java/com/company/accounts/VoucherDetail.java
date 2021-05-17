package com.company.accounts;

public class VoucherDetail {

    private long trx_detail_id;
    private long trx_master_id;
    private int acc_id;
    private double credit;
    private double debit;
    private String narration;
    private int is_active;
    private String in_active_date;
    private int user_id;
    private String create_date;

    public VoucherDetail(){}

    public VoucherDetail(int trx_detail_id, int trx_master_id, int acc_id, double credit,
                         double debit, String narration, int is_active, String in_active_date,
                         int user_id, String create_date) {
        this.trx_detail_id = trx_detail_id;
        this.trx_master_id = trx_master_id;
        this.acc_id = acc_id;
        this.credit = credit;
        this.debit = debit;
        this.narration = narration;
        this.is_active = is_active;
        this.in_active_date = in_active_date;
        this.user_id = user_id;
        this.create_date = create_date;
    }

    public long getTrx_detail_id() {
        return trx_detail_id;
    }

    public void setTrx_detail_id(long trx_detail_id) {
        this.trx_detail_id = trx_detail_id;
    }

    public long getTrx_master_id() {
        return trx_master_id;
    }

    public void setTrx_master_id(long trx_master_id) {
        this.trx_master_id = trx_master_id;
    }

    public int getAcc_id() {
        return acc_id;
    }

    public void setAcc_id(int acc_id) {
        this.acc_id = acc_id;
    }

    public double getCredit() {
        return credit;
    }

    public void setCredit(double credit) {
        this.credit = credit;
    }

    public double getDebit() {
        return debit;
    }

    public void setDebit(double debit) {
        this.debit = debit;
    }

    public String getNarration() {
        return narration;
    }

    public void setNarration(String narration) {
        this.narration = narration;
    }

    public int getIs_active() {
        return is_active;
    }

    public void setIs_active(int is_active) {
        this.is_active = is_active;
    }

    public String getIn_active_date() {
        return in_active_date;
    }

    public void setIn_active_date(String in_active_date) {
        this.in_active_date = in_active_date;
    }

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public String getCreate_date() {
        return create_date;
    }

    public void setCreate_date(String create_date) {
        this.create_date = create_date;
    }
}
