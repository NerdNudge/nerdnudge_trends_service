package com.neurospark.nerdnudge.trends;

import com.neurospark.nerdnudge.metrics.metrics.Metronome;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class NerdnudgeTrendsApplication {
    public static void main(String[] args) {
        Metronome.initiateMetrics(60000);
        SpringApplication.run(NerdnudgeTrendsApplication.class, args);
    }
}