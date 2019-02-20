package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);
    private static RestHighLevelClient client = null;

    public static void main(String[] args) throws IOException, InterruptedException {
        client = ElasticSearchRepository.createClient();

        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        CreateIndexRequest createIndex = new CreateIndexRequest("actors");
        createIndex.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1));

        createIndex.mapping("actor", "name", "type=text");

        try {
            CreateIndexResponse createIndexResponse = client.indices().create(createIndex);
        } catch (ElasticsearchStatusException e) {
            System.err.println("Index already exists");
        }

        BulkProcessor bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {}

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {}

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {}
                })
                .setBulkActions(50000)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(0)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(
                        TimeValue.timeValueMillis(100),
                        3))
                .build();

        String inputFilePath = args[0];
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader
                    .lines()
                    .forEach(line -> {
                        String name = line.replace("\"", "");
                        bulkProcessor.add(new IndexRequest("actors", "actor", "" + count.incrementAndGet())
                                .source(XContentType.JSON, "name", name));
                        System.out.println(name);
                    });
        }

        bulkProcessor.close();

        System.out.println("Inserted total of " + count.get() + " actors");
    }
}
