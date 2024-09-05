package com.neurospark.nerdnudge.trends.config;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.neurospark.nerdnudge.couchbase.service.NerdPersistClient;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class PersistClusterConfig {

    @Getter
    @Value("${persist.connection-string}")
    private String persistConnectionString;

    @Getter
    @Value("${persist.username}")
    private String persistUsername;

    @Getter
    @Value("${persist.password}")
    private String persistPassword;

    @Value("${persist.config.bucket}")
    private String persistConfigBucketName;

    @Value("${persist.config.scope}")
    private String persistConfigScopeName;

    @Value("${persist.config.collection}")
    private String persistConfigCollectionName;

    @Value("${persist.users.bucket}")
    private String persistUsersBucketName;

    @Value("${persist.users.scope}")
    private String persistUsersScopeName;

    @Value("${persist.users.collection}")
    private String persistUsersCollectionName;

    @Bean
    @Primary
    public Cluster cluster() {
        ClusterEnvironment env = ClusterEnvironment.builder().build();
        return Cluster.connect(persistConnectionString, ClusterOptions.clusterOptions(persistUsername, persistPassword).environment(env));
    }

    @Bean(name = "configCollection")
    public Collection configCollection(Cluster cluster) {
        return cluster.bucket(persistConfigBucketName).scope(persistConfigScopeName).collection(persistConfigCollectionName);
    }

    @Bean(name = "configPersist")
    public NerdPersistClient configPersist() {
        return new NerdPersistClient(persistConnectionString, persistUsername, persistPassword, persistConfigBucketName, persistConfigScopeName, persistConfigCollectionName);
    }

    @Bean(name = "userProfilesPersist")
    public NerdPersistClient userProfilesPersist() {
        return new NerdPersistClient(persistConnectionString, persistUsername, persistPassword, persistUsersBucketName, persistUsersScopeName, persistUsersCollectionName);
    }
}
