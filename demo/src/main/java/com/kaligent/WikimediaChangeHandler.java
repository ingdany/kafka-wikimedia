package com.kaligent;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {
    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class);
    KafkaProducer<String, String> producer;
    String topic;
    String key;

    public WikimediaChangeHandler(KafkaProducer<String,String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        throw new UnsupportedOperationException("Unimplemented method");
    }

    @Override
    public void onClosed() throws Exception {
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        String key = "test";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, messageEvent.getData());
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e){
                if (e == null) {
                    log.info(recordMetadata.topic() + ":" + recordMetadata.partition() + ":" + recordMetadata.offset());
                }
            }
        });
    }

    @Override
    public void onComment(String comment) throws Exception {
        throw new UnsupportedOperationException("Unimplemented method");
    }

    @Override
    public void onError(Throwable t) {
        throw new UnsupportedOperationException("Unimplemented method");
    }
}
