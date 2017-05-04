package com.juber.twitter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.SiteStreamsAdapter;
import twitter4j.Status;
import twitter4j.TwitterStream;

public class Test {

    public static void register(TwitterStream twitterStream, KafkaProducer<String, String> kafkaProducer) {
        twitterStream.addListener(new TwitterListener(kafkaProducer));
    }

    private static class TwitterListener extends SiteStreamsAdapter {
        private final KafkaProducer<String, String> kafkaProducer;

        private TwitterListener(KafkaProducer<String, String> kafkaProducer) {
            this.kafkaProducer = kafkaProducer;
        }

        public void onStatus(long forUser, Status status) {
            String text = status.getText();
            String screenName = status.getUser().getScreenName();

            String id = Long.toString(status.getId());
            String message = screenName + ": " + text;
            kafkaProducer.send(new ProducerRecord<>("twitter", id, message));
        }
    }
}
