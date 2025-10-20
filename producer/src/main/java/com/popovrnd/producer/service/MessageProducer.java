package com.popovrnd.producer.service;

import com.popovrnd.producer.service.domain.MyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class MessageProducer {

    private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);

    private final KafkaTemplate<String, MyEvent> kafkaTemplate;
    private final String topicName;

    public MessageProducer(KafkaTemplate<String, MyEvent> kafkaTemplate,
                            @Value("${producer.kafka.topic}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public void sendMessage(String key, MyEvent message) {
        kafkaTemplate.send(topicName, key, message)
                .whenComplete((result, ex) -> {
                    if (ex == null && result != null) {
                        var meta = result.getRecordMetadata();
                        log.info("Sent message to topic='{}' partition={} offset={} key='{}'",
                                meta.topic(), meta.partition(), meta.offset(), key);
                    } else {
                        log.error("Failed to send message with key='{}' to topic='{}': {}",
                                key, topicName, ex.getMessage(), ex);
                    }
                });
    }
}
