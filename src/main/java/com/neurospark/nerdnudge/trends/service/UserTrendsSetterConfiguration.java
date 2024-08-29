package com.neurospark.nerdnudge.trends.service;

import lombok.Data;

@Data
public class UserTrendsSetterConfiguration {
    private String persistAddress;
    private String persistUsername;
    private String persistPassword;

    private String userBucketName;
    private String userScopeName;
    private String userCollectionName;

    private String configBucketName;
    private String configScopeName;
    private String configCollectionName;

    private static UserTrendsSetterConfiguration userTrendsSetterConfiguration = null;

    private UserTrendsSetterConfiguration() {
    }

    public static UserTrendsSetterConfiguration getInstance() {
        if(userTrendsSetterConfiguration == null)
            userTrendsSetterConfiguration = new UserTrendsSetterConfiguration();

        return userTrendsSetterConfiguration;
    }
}
