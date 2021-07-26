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
package org.apache.flink.streaming.connectors.redis.table.sink;


import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.descriptor.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Optional;


public class RedisDynamicTableSink implements DynamicTableSink {

    private final ReadableConfig options;

    private final TableSchema schema;

    private FlinkJedisConfigBase flinkJedisConfigBase;

    private RedisMapper redisMapper;

    private Map<String, String> properties;


    public RedisDynamicTableSink(Map<String, String> properties, TableSchema schema, ReadableConfig options) {
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        this.schema = schema;
        Preconditions.checkNotNull(schema, "tableSchema should not be null");
        this.options = options;
        Preconditions.checkNotNull(options, "options should not be null");

        redisMapper = RedisHandlerServices
                .findRedisHandler(RedisMapperHandler.class, properties)
                .createRedisMapper(properties);
        flinkJedisConfigBase = RedisHandlerServices
                .findRedisHandler(FlinkJedisConfigHandler.class, properties)
                .createFlinkJedisConfig(options);
    }


    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        Preconditions.checkNotNull(options, "No options supplied");

        Preconditions.checkNotNull(flinkJedisConfigBase, "No Jedis config supplied");

        RedisCommand command = RedisCommand.valueOf(options.get(RedisOptions.COMMAND).toUpperCase());

        int fieldCount = schema.getFieldCount();
        if (fieldCount != (needAdditionalKey(command) ? 3 : 2)) {
            throw new ValidationException("Redis sink only supports 2 or 3 columns");
        }

        // TODO incr by 类似的也支持long吧？
        DataType[] dataTypes = schema.getFieldDataTypes();
        for (int i = 0; i < fieldCount; i++) {
            if (!dataTypes[i].getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARCHAR)) {
                throw new ValidationException("Redis connector only supports STRING type");
            }
        }

        RedisMapper<RowData> mapper = new RedisRowDataMapper(options, command);
        RedisSink<RowData> redisSink = new RedisSink<>(flinkJedisConfigBase, mapper);
        return SinkFunctionProvider.of(redisSink);
    }

    private static boolean needAdditionalKey(RedisCommand command) {
        return command.getRedisDataType() == RedisDataType.HASH || command.getRedisDataType() == RedisDataType.SORTED_SET;
    }


    public static final class RedisRowDataMapper implements RedisMapper<RowData> {
        private static final long serialVersionUID = 1L;

        private final ReadableConfig options;
        private final RedisCommand command;

        public RedisRowDataMapper(ReadableConfig options, RedisCommand command) {
            this.options = options;
            this.command = command;
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(command, "default-additional-key");
        }

        @Override
        public String getKeyFromData(RowData data) {
            return data.getString(needAdditionalKey(command) ? 1 : 0).toString();
        }

        @Override
        public String getValueFromData(RowData data) {
            return data.getString(needAdditionalKey(command) ? 2 : 1).toString();
        }

        @Override
        public Optional<String> getAdditionalKey(RowData data) {
            return needAdditionalKey(command) ? Optional.of(data.getString(0).toString()) : Optional.empty();
        }

        @Override
        public Optional<Integer> getAdditionalTTL(RowData data) {
            return options.getOptional(RedisOptions.TTL_SEC);
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(properties, schema, options);
    }

    @Override
    public String asSummaryString() {
        return "Redis Dynamic Table Sink";
    }
}
