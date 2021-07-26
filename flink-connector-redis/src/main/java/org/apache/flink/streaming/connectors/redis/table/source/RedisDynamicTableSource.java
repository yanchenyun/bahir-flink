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

package org.apache.flink.streaming.connectors.redis.table.source;

import org.apache.flink.configuration.ReadableConfig;

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.connectors.redis.table.lookup.RedisRowDataLookupFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;

import java.util.Map;

public class RedisDynamicTableSource implements LookupTableSource {

    private final ReadableConfig options;

    private final TableSchema schema;

    private FlinkJedisConfigBase flinkJedisConfigBase;

    private RedisMapper redisMapper;

    private Map<String, String> properties;


    public RedisDynamicTableSource(Map<String, String> properties, TableSchema schema, ReadableConfig options) {
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
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        Preconditions.checkArgument(context.getKeys().length == 1 && context.getKeys()[0].length == 1, "Redis source only supports lookup by single key");

        int fieldCount = schema.getFieldCount();
        if (fieldCount != 2) {
            throw new ValidationException("Redis source only supports 2 columns");
        }

        DataType[] dataTypes = schema.getFieldDataTypes();
        for (int i = 0; i < fieldCount; i++) {
            if (!dataTypes[i].getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARCHAR)) {
                throw new ValidationException("Redis connector only supports STRING type");
            }
        }

        return TableFunctionProvider.of(new RedisRowDataLookupFunction(options,flinkJedisConfigBase));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(properties, schema, options);
    }

    @Override
    public String asSummaryString() {
        return "Redis Dynamic Table Source";
    }

}
