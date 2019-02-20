package com.serli.oracle.of.bacon.repository;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ElasticSearchRepository {

    private final RestHighLevelClient client;

    public ElasticSearchRepository() {
        client = createClient();

    }

    public static RestHighLevelClient createClient() {
        return new RestHighLevelClient(
            RestClient.builder(
                new HttpHost("localhost", 9200, "http")
            )
        );
    }

    public List<String> getActorsSuggests(String searchQuery) throws IOException {
        SearchRequest search = new SearchRequest("actors");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", searchQuery));
        searchSourceBuilder.size(15);
        searchSourceBuilder.timeout(new TimeValue(10, TimeUnit.SECONDS));
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        search.source(searchSourceBuilder);
        SearchResponse response = client.search(search);

        List<String> result = new ArrayList<>();
        Iterator<SearchHit> it = response.getHits().iterator();
        while (it.hasNext()) {
            SearchHit hit = it.next();
            result.add(hit.getSourceAsMap().get("name").toString());
        }
        return result;
    }
}
