package com.company.accounts;

import java.util.List;

public class AccQueue {
    private List<ChartAccount> chartAccounts;
    private List<Voucher> vouchers;

    public AccQueue(){

    }

    public AccQueue(List<ChartAccount> chartAccounts, List<Voucher> vouchers) {
        this.chartAccounts = chartAccounts;
        this.vouchers = vouchers;
    }

    public List<ChartAccount> getChartAccounts() {
        return chartAccounts;
    }

    public void setChartAccounts(List<ChartAccount> chartAccounts) {
        this.chartAccounts = chartAccounts;
    }

    public List<Voucher> getVouchers() {
        return vouchers;
    }

    public void setVouchers(List<Voucher> vouchers) {
        this.vouchers = vouchers;
    }
}
