package com.popovrnd.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

@Configuration
public class StreamingConfig {

    /**
     * Declares a Kafka topic on application startup.
     * Spring Boot detects this {@link org.apache.kafka.clients.admin.NewTopic} bean
     * and uses {@link org.springframework.kafka.core.KafkaAdmin} (analogous to RabbitAdmin)
     * to create the topic programmatically if it does not exist.
     * <ul>
     *   <li>If the topic exists → creation is silently ignored (idempotent).</li>
     *   <li>If missing → it is created with the specified partitions and replicas.</li>
     *   <li>Default broker retention: 7 days, after which old log segments are deleted.</li>
     * </ul>
     *
     * @param topicName topic name from configuration
     * @return configured {@code NewTopic} bean
     */
    @Bean
    public NewTopic createTopic(@Value("${producer.kafka.topic}") String topicName) {
        return TopicBuilder.name(topicName)
                .partitions(3)                         // Number of partitions
                .replicas(1)                            // Replication factor
                .configs(Map.of(
                        "retention.ms", "604800000",       // Retention duration per topic 7 days
                        "cleanup.policy", "delete"                 // Default deletion policy for expired segments
                ))
                .build();
    }

}
