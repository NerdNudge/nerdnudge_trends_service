package com.neurospark.nerdnudge.trends.service;

import com.couchbase.client.java.query.QueryResult;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.neurospark.nerdnudge.couchbase.service.NerdPersistClient;
import com.neurospark.nerdnudge.trends.dto.UserEntity;
import com.neurospark.nerdnudge.trends.utils.Commons;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class UserTrendsSetter {
    @Autowired
    private NerdPersistClient userProfilesPersist;

    @Autowired
    private NerdPersistClient configPersist;

    private List<String> topics = new ArrayList<>();
    private JsonParser jsonParser = new JsonParser();
    private int pageSize = 5000;
    private Map<String, UserEntity> userEntities;

    @Value("${persist.users.bucket}")
    private String persistUsersBucketName;

    @Value("${persist.users.scope}")
    private String persistUsersScopeName;

    @Value("${persist.users.collection}")
    private String persistUsersCollectionName;

    @Scheduled(fixedDelayString = "${user.trends.refresh.frequency}")
    public void updateUserTrends() {
        System.out.println("--------------------NERD NUDGE USER TRENDS UPDATE -> START--------------------");
        try {
            fetchUsers();
            saveUserTrendsToPersist();
        } catch (Exception ex) {
            System.out.println("[ERROR] Issue Updating User Trends");
            ex.printStackTrace();
        }
        System.out.println("--------------------NERD NUDGE USER TRENDS UPDATE -> END--------------------");
    }

    private void fetchUsers() {
        userEntities = new HashMap<>();
        List<String> allTopics = getAllTopics();
        for(int i = 0; i < allTopics.size(); i ++) {
            String currentTopic = allTopics.get(i);
            int totalPages = getTotalPages(currentTopic);
            int topicRank = 0;
            System.out.println(new Date() + "Fetching for topic: " + currentTopic + ", total Pages: " + totalPages);
            for (int k = 1; k <= totalPages; k++) {
                int offset = (k - 1) * pageSize;
                String topicRankQuery = getTopicRankQueryString(currentTopic, offset);
                QueryResult result = userProfilesPersist.getDocumentsByQuery(topicRankQuery);
                for (com.couchbase.client.java.json.JsonObject row : result.rowsAsObject()) {
                    JsonObject thisResult = jsonParser.parse(row.toString()).getAsJsonObject();
                    topicRank++;
                    String userId = thisResult.get("userId").getAsString();
                    double score = thisResult.get("score").getAsDouble();

                    UserEntity userEntity = userEntities.getOrDefault(userId, new UserEntity());
                    userEntity.setUserId(userId);
                    if(userEntity.getTopicsRank() == null) {
                        userEntity.setTopicsRank(new HashMap<>());
                        userEntity.setTopicsScore(new HashMap<>());
                    }

                    if(!userEntity.getTopicsRank().containsKey(currentTopic) || userEntity.getTopicsRank().get(currentTopic) > topicRank)
                        userEntity.getTopicsRank().put(currentTopic, topicRank);

                    if(!userEntity.getTopicsScore().containsKey(currentTopic) || userEntity.getTopicsScore().get(currentTopic) < topicRank)
                        userEntity.getTopicsScore().put(currentTopic, score);

                    userEntities.put(userId, userEntity);

                    if(topicRank <= 10)
                        System.out.println(userEntity);
                }
            }
        }
    }

    private void saveUserTrendsToPersist() {
        Iterator<Map.Entry<String, UserEntity>> usersIterator = userEntities.entrySet().iterator();
        while(usersIterator.hasNext()) {
            Map.Entry<String, UserEntity> thisEntry = usersIterator.next();
            String userId = thisEntry.getKey();
            UserEntity thisUserEntity = thisEntry.getValue();

            JsonObject userTrends = userProfilesPersist.get(userId + "-trends");
            if(userTrends == null) {
                userTrends = new JsonObject();
                userTrends.addProperty("type", "userTrends");
            }

            JsonObject trendsObject = (userTrends.has("trends")) ? userTrends.get("trends").getAsJsonObject() : new JsonObject();
            Map<String, Integer> userTopicRanks = thisUserEntity.getTopicsRank();
            Map<String, Double> userTopicScores = thisUserEntity.getTopicsScore();

            Iterator<Map.Entry<String, Integer>> userTopicRanksIterator = userTopicRanks.entrySet().iterator();
            while(userTopicRanksIterator.hasNext()) {
                Map.Entry<String, Integer> thisTopicRankEntry = userTopicRanksIterator.next();
                String currentTopicId = thisTopicRankEntry.getKey();
                JsonObject currentTopicTrend = (trendsObject.has(currentTopicId)) ? trendsObject.get(currentTopicId).getAsJsonObject() : new JsonObject();
                JsonArray thisDayTrend = new JsonArray();
                thisDayTrend.add(userTopicScores.get(currentTopicId));
                thisDayTrend.add(userTopicRanks.get(currentTopicId));

                currentTopicTrend.add(Commons.getDaystamp(), thisDayTrend);
                Commons.housekeepDayJsonObject(currentTopicTrend, 30);
                trendsObject.add(currentTopicId, currentTopicTrend);
            }
            userTrends.add("trends", trendsObject);
            userProfilesPersist.set(userId + "-trends", userTrends);
        }
    }


    private String getTopicRankQueryString(String topic, int offset) {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT META().id as userId, scores.");
        queryBuilder.append(topic);
        queryBuilder.append(" AS score");
        queryBuilder.append(" FROM `");
        queryBuilder.append(persistUsersBucketName);
        queryBuilder.append("`.`");
        queryBuilder.append(persistUsersScopeName);
        queryBuilder.append("`.`");
        queryBuilder.append(persistUsersCollectionName);
        queryBuilder.append("`");
        queryBuilder.append(" WHERE scores.");
        queryBuilder.append(topic);
        queryBuilder.append(" IS NOT NULL");
        queryBuilder.append(" ORDER BY scores.");
        queryBuilder.append(topic);
        queryBuilder.append(" DESC");
        queryBuilder.append(" LIMIT ");
        queryBuilder.append(pageSize);
        queryBuilder.append(" OFFSET ");
        queryBuilder.append(offset);

        return queryBuilder.toString();
    }

    private int getTotalPages(String topic) {
        String queryString = getCountsQuery(topic);
        System.out.println(queryString);
        QueryResult result = userProfilesPersist.getDocumentsByQuery(queryString);
        for (com.couchbase.client.java.json.JsonObject row : result.rowsAsObject()) {
            JsonObject thisResult = jsonParser.parse(row.toString()).getAsJsonObject();
            if(thisResult.has("count")) {
                System.out.println("TOPIC: " + topic + ":::: COUNTS: " + thisResult.get("count").getAsInt());
                return (int) Math.ceil((double) thisResult.get("count").getAsInt() / pageSize);
            }
        }
        return 1;
    }

    private String getCountsQuery(String topic) {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT COUNT(1) AS count FROM `");
        queryBuilder.append(persistUsersBucketName);
        queryBuilder.append("`.`");
        queryBuilder.append(persistUsersScopeName);
        queryBuilder.append("`.`");
        queryBuilder.append(persistUsersCollectionName);
        queryBuilder.append("` ");
        queryBuilder.append("WHERE scores.");
        queryBuilder.append(topic);
        queryBuilder.append(" IS NOT NULL");

        return queryBuilder.toString();
    }


    private List<String> getAllTopics() {
        System.out.println("getting all topics.");
        JsonObject topicCodeToTopicNameMapping = configPersist.get("collection_topic_mapping");
        List<String> allTopics = new ArrayList<>();
        allTopics.add("global");

        Iterator<Map.Entry<String, JsonElement>> topicsIterator = topicCodeToTopicNameMapping.entrySet().iterator();
        while(topicsIterator.hasNext()) {
            Map.Entry<String, JsonElement> thisEntry = topicsIterator.next();
            allTopics.add(thisEntry.getKey());
        }

        return allTopics;
    }
}
