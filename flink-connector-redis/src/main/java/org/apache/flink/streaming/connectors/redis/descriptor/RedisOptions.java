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
package org.apache.flink.streaming.connectors.redis.descriptor;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import redis.clients.jedis.Protocol;

public class RedisOptions {

    private RedisOptions() {
    }

    public static final ConfigOption<String> MODE = ConfigOptions
            .key("mode")
            .stringType()
            .defaultValue("single");

    /**
     * redis模式 MODE 的枚举值
     */
    public enum Mode {
        SINGLE("single"), SENTIAL("sential"), CLUSTER("cluster");
        private String mode;

        Mode(String mode) {
            this.mode = mode;
        }

        @Override
        public String toString() {
            return this.mode;
        }
    }

    public static final ConfigOption<String> SINGLE_HOST = ConfigOptions
            .key("single.host")
            .stringType()
            .defaultValue(Protocol.DEFAULT_HOST);

    public static final ConfigOption<Integer> SINGLE_PORT = ConfigOptions
            .key("single.port")
            .intType()
            .defaultValue(Protocol.DEFAULT_PORT);

    public static final ConfigOption<String> CLUSTER_NODES = ConfigOptions
            .key("cluster.nodes")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> SENTINEL_NODES = ConfigOptions
            .key("sentinel.nodes")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> SENTINEL_MASTER = ConfigOptions
            .key("sentinel.master")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> COMMAND = ConfigOptions
            .key("command")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> DB_NUM = ConfigOptions
            .key("db-num")
            .intType()
            .defaultValue(Protocol.DEFAULT_DATABASE);

    public static final ConfigOption<Integer> TTL_SEC = ConfigOptions
            .key("ttl-sec")
            .intType()
            .noDefaultValue();

    public static final ConfigOption<Integer> CONNECTION_TIMEOUT_MS = ConfigOptions
            .key("connection.timeout-ms")
            .intType()
            .defaultValue(Protocol.DEFAULT_TIMEOUT);

    public static final ConfigOption<Integer> CONNECTION_MAX_TOTAL = ConfigOptions
            .key("connection.max-total")
            .intType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL);

    public static final ConfigOption<Integer> CONNECTION_MAX_IDLE = ConfigOptions
            .key("connection.max-idle")
            .intType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_MAX_IDLE);

    public static final ConfigOption<Integer> CONNECTION_MIN_IDLE = ConfigOptions
            .key("connection.min-idle")
            .intType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_MIN_IDLE);

    public static final ConfigOption<Boolean> CONNECTION_TEST_ON_BORROW = ConfigOptions
            .key("connection.test-on-borrow")
            .booleanType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_TEST_ON_BORROW);

    public static final ConfigOption<Boolean> CONNECTION_TEST_ON_RETURN = ConfigOptions
            .key("connection.test-on-return")
            .booleanType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_TEST_ON_RETURN);

    public static final ConfigOption<Boolean> CONNECTION_TEST_WHILE_IDLE = ConfigOptions
            .key("connection.test-while-idle")
            .booleanType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE);

    public static final ConfigOption<Integer> CONNECTION_MAX_REDIRECTIONS = ConfigOptions
            .key("connection.max-redirections")
            .intType()
            .defaultValue(5);

    public static final ConfigOption<String> LOOKUP_ADDITIONAL_KEY = ConfigOptions
            .key("lookup.additional-key")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
            .key("lookup.cache.max-rows")
            .intType()
            .defaultValue(-1);

    public static final ConfigOption<Integer> LOOKUP_CACHE_TTL_SEC = ConfigOptions
            .key("lookup.cache.ttl-sec")
            .intType()
            .defaultValue(-1);

}
