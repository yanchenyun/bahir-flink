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
package org.apache.flink.streaming.connectors.redis;


import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.descriptor.RedisOptions;
import org.apache.flink.streaming.connectors.redis.table.sink.RedisDynamicTableSink;
import org.apache.flink.streaming.connectors.redis.table.source.RedisDynamicTableSource;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.StringUtils;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;


public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "redis";


    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateOptions(config);
        return new RedisDynamicTableSource(context.getCatalogTable().getOptions(), context.getCatalogTable().getSchema(), config);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateOptions(config);
        return new RedisDynamicTableSink(context.getCatalogTable().getOptions(), context.getCatalogTable().getSchema(), config);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(RedisOptions.MODE);
        requiredOptions.add(RedisOptions.COMMAND);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(RedisOptions.SINGLE_HOST);
        optionalOptions.add(RedisOptions.SINGLE_PORT);
        optionalOptions.add(RedisOptions.CLUSTER_NODES);
        optionalOptions.add(RedisOptions.SENTINEL_NODES);
        optionalOptions.add(RedisOptions.SENTINEL_MASTER);
        optionalOptions.add(RedisOptions.PASSWORD);
        optionalOptions.add(RedisOptions.DB_NUM);
        optionalOptions.add(RedisOptions.TTL_SEC);
        optionalOptions.add(RedisOptions.CONNECTION_TIMEOUT_MS);
        optionalOptions.add(RedisOptions.CONNECTION_MAX_TOTAL);
        optionalOptions.add(RedisOptions.CONNECTION_MAX_IDLE);
        optionalOptions.add(RedisOptions.CONNECTION_MIN_IDLE);
        optionalOptions.add(RedisOptions.CONNECTION_TEST_ON_BORROW);
        optionalOptions.add(RedisOptions.CONNECTION_TEST_ON_RETURN);
        optionalOptions.add(RedisOptions.CONNECTION_TEST_WHILE_IDLE);
        optionalOptions.add(RedisOptions.CONNECTION_MAX_REDIRECTIONS);
        optionalOptions.add(RedisOptions.LOOKUP_ADDITIONAL_KEY);
        optionalOptions.add(RedisOptions.LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(RedisOptions.LOOKUP_CACHE_TTL_SEC);
        return optionalOptions;
    }


    /**
     * valid requiredOptions which base on redis mode
     *
     * @param options
     */
    private void validateOptions(ReadableConfig options) {
        String mode = options.get(RedisOptions.MODE);

        if (RedisOptions.Mode.SINGLE.toString().equals(mode)) {
            if (StringUtils.isNullOrWhitespaceOnly(options.get(RedisOptions.SINGLE_HOST))) {
                throw new IllegalArgumentException("Parameter single.host must be provided in single mode");
            }
        } else if (RedisOptions.Mode.SENTIAL.toString().equals(mode)) {
            if (StringUtils.isNullOrWhitespaceOnly(options.get(RedisOptions.SENTINEL_NODES))
                    || StringUtils.isNullOrWhitespaceOnly(options.get(RedisOptions.SENTINEL_MASTER))) {
                throw new IllegalArgumentException("Parameters sentinel.nodes and sentinel.master must be provided in sentinel mode");
            }
        } else if (RedisOptions.Mode.CLUSTER.toString().equals(mode)) {
            if (StringUtils.isNullOrWhitespaceOnly(options.get(RedisOptions.CLUSTER_NODES))) {
                throw new IllegalArgumentException("Parameter cluster.nodes must be provided in cluster mode");
            }
        } else {
            throw new IllegalArgumentException("Invalid Redis mode. Must be single/cluster/sentinel");
        }

    }
}