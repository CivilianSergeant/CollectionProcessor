package com.company;

import java.util.List;

public class TabCollection{

    private String CollectionDate;

    private String UserId;

    private int APIVersion;

    private List<Collection> Collections;

    TabCollection(){

    }

    public TabCollection(String collectionDate, String userId,  int APIVersion, List<Collection> collections) {
        this.CollectionDate = collectionDate;
        this.UserId = userId;
        this.APIVersion = APIVersion;
        this.Collections = collections;
    }

    public String getUserId() {
        return UserId;
    }

    public void setUserId(String userId) {
        UserId = userId;
    }

    public String getCollectionDate() {
        return CollectionDate;
    }

    public void setCollectionDate(String collectionDate) {
        CollectionDate = collectionDate;
    }



    public int getAPIVersion() {
        return APIVersion;
    }

    public void setAPIVersion(int APIVersion) {
        this.APIVersion = APIVersion;
    }

    public List<Collection> getCollections() {
        return Collections;
    }

    public void setCollections(List<Collection> collections) {
        this.Collections = collections;
    }
}
