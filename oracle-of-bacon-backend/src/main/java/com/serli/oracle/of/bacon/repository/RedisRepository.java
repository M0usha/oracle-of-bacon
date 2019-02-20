package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {
    private final Jedis jedis;
    private final String key = "miamLeBacon";

    public RedisRepository() {
        this.jedis = new Jedis("localhost");
    }

    public List<String> getLastTenSearches() {
        return jedis.lrange(this.key, 0, -1);
    }

    public void add(String search){
        jedis.lpush(this.key,search);
        jedis.ltrim(this.key,0,9);
    }

}