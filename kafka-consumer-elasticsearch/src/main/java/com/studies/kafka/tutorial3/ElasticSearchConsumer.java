package com.studies.kafka.tutorial3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ElasticSearchConsumer {

    Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public ElasticSearchConsumer(){}

    public void run(){
        logger.info("Started ElasticSearchConsumer...");
    }
}
