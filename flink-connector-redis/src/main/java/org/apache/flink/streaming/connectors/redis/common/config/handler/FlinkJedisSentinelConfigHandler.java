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

import java.util.*;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.streaming.connectors.redis.descriptor.RedisOptions;
import org.apache.flink.util.Preconditions;

public class FlinkJedisSentinelConfigHandler implements FlinkJedisConfigHandler {

    @Override
    public FlinkJedisConfigBase createFlinkJedisConfig(ReadableConfig config) {
        String masterName = config.get(RedisOptions.SENTINEL_MASTER);
        String sentinelsInfo = config.get(RedisOptions.SENTINEL_NODES);

        Preconditions.checkNotNull(masterName, "master should not be null in sentinel mode");
        Preconditions.checkNotNull(sentinelsInfo, "sentinels should not be null in sentinel mode");

        Set<String> sentinels = new HashSet<>(Arrays.asList(sentinelsInfo.split(",")));
        FlinkJedisSentinelConfig.Builder builder = new FlinkJedisSentinelConfig.Builder()
                .setMasterName(masterName)
                .setSentinels(sentinels);

        String password = config.get(RedisOptions.PASSWORD);
        if (Objects.nonNull(password)) {
            builder.setPassword(password);
        }
        return builder.build();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(RedisOptions.MODE.key(), RedisOptions.Mode.SENTIAL.toString());
        return require;
    }

    public FlinkJedisSentinelConfigHandler() {

    }
}
