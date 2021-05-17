package com.company;

public class Collection {

    private int CollectionID;
    private String ProductName;
    private String CenterName;
    private String OfficeName;
    private double Amount;
    private String MemberCode;
    private int ProductID;
    private int CenterID;
    private int OfficeID;
    private long MemberID;
    private double DueAmount;
    private int TrxType;
    private int ProductType;
    private int TType;
    private int PType;
    private int SyncFlag;
    private String Token;
    private long Sid;
    private long SummaryID;
    private double IntCharge;
    private double LoanInstallment;
    private double IntInstallment;
    private int CollectionType;
    private String Created;
    private double fine;


    public Collection() {

    }

    public Collection(int collectionID, String productName, String centerName, String officeName, double amount,
                      String memberCode, int productID, int centerID, int officeID, long memberID, double dueAmount,
                      int tType, int pType, int syncFlag, String token, long sid, double intCharge,
                      double loanInstallment, double intInstallment, int collectionType, String created,double fine) {
        CollectionID = collectionID;
        ProductName = productName;
        CenterName = centerName;
        OfficeName = officeName;
        Amount = amount;
        MemberCode = memberCode;
        ProductID = productID;
        CenterID = centerID;
        OfficeID = officeID;
        MemberID = memberID;
        DueAmount = dueAmount;
        TType = tType;
        TrxType = TType;
        PType = pType;
        ProductType=PType;
        SyncFlag = syncFlag;
        Token = token;
        Sid = sid;
        SummaryID = Sid;
        IntCharge = intCharge;
        LoanInstallment = loanInstallment;
        IntInstallment = intInstallment;
        CollectionType = collectionType;
        Created = created;
        this.fine = fine;
    }

    public int getCollectionID() {
        return CollectionID;
    }

    public void setCollectionID(int collectionID) {
        CollectionID = collectionID;
    }

    public String getProductName() {
        return ProductName;
    }

    public void setProductName(String productName) {
        ProductName = productName;
    }

    public String getCenterName() {
        return CenterName;
    }

    public void setCenterName(String centerName) {
        CenterName = centerName;
    }

    public String getOfficeName() {
        return OfficeName;
    }

    public void setOfficeName(String officeName) {
        OfficeName = officeName;
    }

    public double getAmount() {
        return Amount;
    }

    public void setAmount(double amount) {
        Amount = amount;
    }

    public String getMemberCode() {
        return MemberCode;
    }

    public void setMemberCode(String memberCode) {
        MemberCode = memberCode;
    }

    public int getProductID() {
        return ProductID;
    }

    public void setProductID(int productID) {
        ProductID = productID;
    }

    public int getCenterID() {
        return CenterID;
    }

    public void setCenterID(int centerID) {
        CenterID = centerID;
    }

    public int getOfficeID() {
        return OfficeID;
    }

    public void setOfficeID(int officeID) {
        OfficeID = officeID;
    }

    public long getMemberID() {
        return MemberID;
    }

    public void setMemberID(long memberID) {
        MemberID = memberID;
    }

    public double getDueAmount() {
        return DueAmount;
    }

    public void setDueAmount(double dueAmount) {
        DueAmount = dueAmount;
    }

    public int getTrxType() {
        return TrxType;
    }

    public void setTrxType(int trxType) {
        TrxType = trxType;
    }

    public int getProductType() {
        return ProductType;
    }

    public void setProductType(int productType) {
        ProductType = productType;
    }

    public int getSyncFlag() {
        return SyncFlag;
    }

    public void setSyncFlag(int syncFlag) {
        SyncFlag = syncFlag;
    }

    public String getToken() {
        return Token;
    }

    public void setToken(String token) {
        Token = token;
    }

    public long getSummaryID() {
        return SummaryID;
    }

    public void setSummaryID(long summaryID) {
        SummaryID = summaryID;
    }

    public double getIntCharge() {
        return IntCharge;
    }

    public void setIntCharge(double intCharge) {
        IntCharge = intCharge;
    }

    public double getLoanInstallment() {
        return LoanInstallment;
    }

    public void setLoanInstallment(double loanInstallment) {
        LoanInstallment = loanInstallment;
    }

    public double getIntInstallment() {
        return IntInstallment;
    }

    public void setIntInstallment(double intInstallment) {
        IntInstallment = intInstallment;
    }

    public int getCollectionType() {
        return CollectionType;
    }

    public void setCollectionType(int collectionType) {
        CollectionType = collectionType;
    }

    public String getCreated() {
        return Created;
    }

    public void setCreated(String created) {
        Created = created;
    }

    public int getTType() {
        return TType;
    }

    public void setTType(int TType) {
        this.TType = TType;
    }

    public int getPType() {
        return PType;
    }

    public void setPType(int PType) {
        this.PType = PType;
    }

    public long getSid() {
        return Sid;
    }

    public void setSid(long sid) {
        Sid = sid;
    }

    public double getFine() {
        return fine;
    }

    public void setFine(double fine) {
        this.fine = fine;
    }
}
