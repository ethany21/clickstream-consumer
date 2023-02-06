package com.github.ethany21.clickstreamconsumer.kafka.consumer;

import clickstream.events;
import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkTask;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

@Component
public class Consumer {
    private final ElasticsearchAsyncClient elasticsearchAsyncClient;

    public Consumer(ElasticsearchAsyncClient elasticsearchAsyncClient) {
        this.elasticsearchAsyncClient = elasticsearchAsyncClient;

        SinkConnector sinkConnector;
        SinkTask sinkTask;
        AbstractConfig abstractConfig;
    }

    private static final Logger LOGGER = Logger.getLogger(Consumer.class.getName());

    @KafkaListener(topics = "clickstream", containerFactory = "clickStreamKafkaListenerContainerFactory")
    public void listener(List<events> eventRecords) throws ExecutionException, InterruptedException {
        if (eventRecords.size() > 0) {
            BulkRequest.Builder bulkRequest = new BulkRequest.Builder();
            for (events event : eventRecords) {
                Map<String, Object> map = new HashMap<>();
                event.getSchema().getFields().forEach(field ->
                        map.put(field.name(), event.get(field.name())));

                bulkRequest.operations(op -> op
                        .index(idx -> idx
                                .index("clickstream")
                                .document(map)
                        )
                );
            }
            elasticsearchAsyncClient.bulk(bulkRequest.build());
        }
    }
}
