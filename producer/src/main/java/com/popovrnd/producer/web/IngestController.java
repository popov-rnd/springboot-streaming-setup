package com.popovrnd.producer.web;

import com.popovrnd.producer.service.MessageProducer;
import com.popovrnd.producer.service.domain.MyEvent;
import com.popovrnd.producer.web.request.MyEventRequest;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IngestController {

    private static final Logger log = LoggerFactory.getLogger(IngestController.class);

    private final MessageProducer producer;

    public IngestController(MessageProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/ingest")
    public String sendMessage(@Valid @RequestBody MyEventRequest request) {

        MyEvent event = MyEvent.of(
                request.type(),
                request.source(),
                request.payload()
        );

        producer.sendMessage(event.id(), event);
        log.debug("Event has been sent! {}", event);
        return "Sent: " + event;
    }

}
