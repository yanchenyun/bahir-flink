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
package org.apache.flink.streaming.connectors.redis.common.config.handler;


import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.streaming.connectors.redis.descriptor.RedisOptions;
import org.apache.flink.util.Preconditions;

/**
 * jedis cluster config handler to find and create jedis cluster config use meta.
 */
public class FlinkJedisClusterConfigHandler implements FlinkJedisConfigHandler {

    @Override
    public FlinkJedisConfigBase createFlinkJedisConfig(ReadableConfig config) {
        String nodesInfo = config.get(RedisOptions.CLUSTER_NODES);
        Preconditions.checkNotNull(nodesInfo, "nodes should not be null in cluster mode");

        Set<InetSocketAddress> nodes = Arrays.stream(nodesInfo.split(",")).map(r -> {
            String[] arr = r.split(":");
            return new InetSocketAddress(arr[0].trim(), Integer.parseInt(arr[1].trim()));
        }).collect(Collectors.toSet());

        FlinkJedisClusterConfig.Builder builder = new FlinkJedisClusterConfig.Builder()
                .setNodes(nodes)
                .setMaxIdle(config.get(RedisOptions.CONNECTION_MAX_IDLE))
                .setMaxTotal(config.get(RedisOptions.CONNECTION_MAX_TOTAL))
                .setTimeout(config.get(RedisOptions.CONNECTION_TIMEOUT_MS));

        String password = config.get(RedisOptions.PASSWORD);
        if (Objects.nonNull(password)) {
            builder.setPassword(password);
        }
        return builder.build();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(RedisOptions.MODE.key(), RedisOptions.Mode.CLUSTER.toString());
        return require;
    }

    public FlinkJedisClusterConfigHandler() {
    }
}
