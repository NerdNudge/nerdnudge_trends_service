package com.neurospark.nerdnudge.trends.dto;

import lombok.Data;

import java.util.Map;

@Data
public class UserEntity {
    private String userId;
    private Map<String, Integer> topicsRank;
    private Map<String, Double> topicsScore;
}
