/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.redis.common.container;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.util.Preconditions;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;


/**
 * The builder for {@link RedisCommandsContainer}.
 */
public class RedisCommandsContainerBuilder {

    /**
     * Initialize the {@link RedisCommandsContainer} based on the instance type.
     *
     * @param flinkJedisConfigBase configuration base
     * @return @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and jedisSentinelConfig are all null
     */
    public static RedisCommandsContainer build(FlinkJedisConfigBase flinkJedisConfigBase) {
        if (flinkJedisConfigBase instanceof FlinkJedisPoolConfig) {
            FlinkJedisPoolConfig flinkJedisPoolConfig = (FlinkJedisPoolConfig) flinkJedisConfigBase;
            return RedisCommandsContainerBuilder.build(flinkJedisPoolConfig);
        } else if (flinkJedisConfigBase instanceof FlinkJedisClusterConfig) {
            FlinkJedisClusterConfig flinkJedisClusterConfig = (FlinkJedisClusterConfig) flinkJedisConfigBase;
            return RedisCommandsContainerBuilder.build(flinkJedisClusterConfig);
        } else if (flinkJedisConfigBase instanceof FlinkJedisSentinelConfig) {
            FlinkJedisSentinelConfig flinkJedisSentinelConfig = (FlinkJedisSentinelConfig) flinkJedisConfigBase;
            return RedisCommandsContainerBuilder.build(flinkJedisSentinelConfig);
        } else {
            throw new IllegalArgumentException("Jedis configuration not found");
        }
    }

    /**
     * Builds container for single Redis environment.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @return container for single Redis environment
     */
    public static RedisCommandsContainer build(FlinkJedisPoolConfig jedisPoolConfig) {
        Preconditions.checkNotNull(jedisPoolConfig, "Redis pool config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig = getGenericObjectPoolConfig(jedisPoolConfig);

        JedisPool jedisPool = new JedisPool(
                genericObjectPoolConfig,
                jedisPoolConfig.getHost(),
                jedisPoolConfig.getPort(),
                jedisPoolConfig.getConnectionTimeout(),
                jedisPoolConfig.getPassword(),
                jedisPoolConfig.getDatabase());
        return new RedisContainer(jedisPool);
    }

    /**
     * Builds container for Redis Cluster environment.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @return container for Redis Cluster environment
     */
    public static RedisCommandsContainer build(FlinkJedisClusterConfig jedisClusterConfig) {
        Preconditions.checkNotNull(jedisClusterConfig, "Redis cluster config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig = getGenericObjectPoolConfig(jedisClusterConfig);

        JedisCluster jedisCluster = new JedisCluster(
                jedisClusterConfig.getNodes(),
                jedisClusterConfig.getConnectionTimeout(),
                jedisClusterConfig.getConnectionTimeout(),
                jedisClusterConfig.getMaxRedirections(),
                jedisClusterConfig.getPassword(),
                genericObjectPoolConfig);
        return new RedisClusterContainer(jedisCluster);
    }

    /**
     * Builds container for Redis Sentinel environment.
     *
     * @param jedisSentinelConfig configuration for JedisSentinel
     * @return container for Redis sentinel environment
     */
    public static RedisCommandsContainer build(FlinkJedisSentinelConfig jedisSentinelConfig) {
        Preconditions.checkNotNull(jedisSentinelConfig, "Redis sentinel config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig = getGenericObjectPoolConfig(jedisSentinelConfig);

        JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(
                jedisSentinelConfig.getMasterName(),
                jedisSentinelConfig.getSentinels(),
                genericObjectPoolConfig,
                jedisSentinelConfig.getConnectionTimeout(),
                jedisSentinelConfig.getSoTimeout(),
                jedisSentinelConfig.getPassword(),
                jedisSentinelConfig.getDatabase());
        return new RedisContainer(jedisSentinelPool);
    }

    public static GenericObjectPoolConfig getGenericObjectPoolConfig(FlinkJedisConfigBase jedisConfig) {
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisConfig.getMinIdle());
        genericObjectPoolConfig.setTestOnBorrow(jedisConfig.getTestOnBorrow());
        genericObjectPoolConfig.setTestOnReturn(jedisConfig.getTestOnReturn());

        return genericObjectPoolConfig;
    }
}
