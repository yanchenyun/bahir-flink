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
package org.apache.flink.streaming.connectors.redis.common.config;

import org.apache.flink.streaming.connectors.redis.descriptor.RedisOptions;
import org.apache.flink.util.Preconditions;


/**
 * Configuration for Jedis pool.
 */
public class FlinkJedisPoolConfig extends FlinkJedisConfigBase {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final int database;


    /**
     * Jedis pool configuration.
     * The host is mandatory, and when host is not set, it throws NullPointerException.
     *
     * @param host hostname or IP
     * @param port port, default value is 6379
     * @param connectionTimeout socket / connection timeout, default value is 2000 milli second
     * @param password password, if any
     * @param database database index
     * @param maxTotal the maximum number of objects that can be allocated by the pool, default value is 8
     * @param maxIdle the cap on the number of "idle" instances in the pool, default value is 8
     * @param minIdle the minimum number of idle objects to maintain in the pool, default value is 0
     * @param testOnBorrow Whether objects borrowed from the pool will be validated before being returned, default value is false
     * @param testOnReturn Whether objects borrowed from the pool will be validated when they are returned to the pool, default value is false
     * @param testWhileIdle Whether objects sitting idle in the pool will be validated by the idle object evictor, default value is false
     * @throws NullPointerException if parameter {@code host} is {@code null}
     */
    private FlinkJedisPoolConfig(String host, int port, int connectionTimeout, String password, int database,
                                 int maxTotal, int maxIdle, int minIdle,
                                 boolean testOnBorrow, boolean testOnReturn, boolean testWhileIdle) {
        super(connectionTimeout, maxTotal, maxIdle, minIdle, password, testOnBorrow, testOnReturn, testWhileIdle);

        Preconditions.checkNotNull(host, "Host information should be presented");
        this.host = host;
        this.port = port;
        this.database = database;
    }

    /**
     * Returns host.
     *
     * @return hostname or IP
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns port.
     *
     * @return port
     */
    public int getPort() {
        return port;
    }


    /**
     * Returns database index.
     *
     * @return database index
     */
    public int getDatabase() {
        return database;
    }

    /**
     * Builder for initializing  {@link FlinkJedisPoolConfig}.
     */
    public static class Builder {
        private String host = RedisOptions.SINGLE_HOST.defaultValue();
        private int port = RedisOptions.SINGLE_PORT.defaultValue();
        private int timeout = RedisOptions.CONNECTION_TIMEOUT_MS.defaultValue();
        private int database = RedisOptions.DB_NUM.defaultValue();
        private String password;
        private int maxTotal = RedisOptions.CONNECTION_MAX_TOTAL.defaultValue();
        private int maxIdle = RedisOptions.CONNECTION_MAX_IDLE.defaultValue();
        private int minIdle = RedisOptions.CONNECTION_MIN_IDLE.defaultValue();
        private boolean testOnBorrow = RedisOptions.CONNECTION_TEST_ON_BORROW.defaultValue();
        private boolean testOnReturn = RedisOptions.CONNECTION_TEST_ON_RETURN.defaultValue();
        private boolean testWhileIdle = RedisOptions.CONNECTION_TEST_WHILE_IDLE.defaultValue();

        /**
         * Sets value for the {@code maxTotal} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param maxTotal maxTotal the maximum number of objects that can be allocated by the pool, default value is 8
         * @return Builder itself
         */
        public Builder setMaxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        /**
         * Sets value for the {@code maxIdle} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param maxIdle the cap on the number of "idle" instances in the pool, default value is 8
         * @return Builder itself
         */
        public Builder setMaxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        /**
         * Sets value for the {@code minIdle} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param minIdle the minimum number of idle objects to maintain in the pool, default value is 0
         * @return Builder itself
         */
        public Builder setMinIdle(int minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        /**
         * Sets host.
         *
         * @param host host
         * @return Builder itself
         */
        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets port.
         *
         * @param port port, default value is 6379
         * @return Builder itself
         */
        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets timeout.
         *
         * @param timeout timeout, default value is 2000
         * @return Builder itself
         */
        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Sets database index.
         *
         * @param database database index, default value is 0
         * @return Builder itself
         */
        public Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        /**
         * Sets password.
         *
         * @param password password, if any
         * @return Builder itself
         */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * Sets value for the {@code testOnBorrow} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param testOnBorrow Whether objects borrowed from the pool will be validated before being returned
         * @return Builder itself
         */
        public Builder setTestOnBorrow(boolean testOnBorrow) {
            this.testOnBorrow = testOnBorrow;
            return this;
        }

        /**
         * Sets value for the {@code testOnReturn} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param testOnReturn Whether objects borrowed from the pool will be validated when they are returned to the pool
         * @return Builder itself
         */
        public Builder setTestOnReturn(boolean testOnReturn) {
            this.testOnReturn = testOnReturn;
            return this;
        }

        /**
         * Sets value for the {@code testWhileIdle} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * Setting this to true will also set default idle-testing parameters provided in Jedis
         * @see redis.clients.jedis.JedisPoolConfig
         *
         * @param testWhileIdle Whether objects sitting idle in the pool will be validated by the idle object evictor
         * @return Builder itself
         */
        public Builder setTestWhileIdle(boolean testWhileIdle) {
            this.testWhileIdle = testWhileIdle;
            return this;
        }

        /**
         * Builds JedisPoolConfig.
         *
         * @return JedisPoolConfig
         */
        public FlinkJedisPoolConfig build() {
            return new FlinkJedisPoolConfig(host, port, timeout, password, database, maxTotal, maxIdle, minIdle, testOnBorrow, testOnReturn, testWhileIdle);
        }
    }

    @Override
    public String toString() {
        return "FlinkJedisPoolConfig{" +
          "host=" + host +
          ", port=" + port +
          ", database=" + database +
          ", maxTotal=" + maxTotal +
          ", maxIdle=" + maxIdle +
          ", minIdle=" + minIdle +
          ", connectionTimeout=" +
          ", password=" + password +
          ", testOnBorrow=" + testOnBorrow +
          ", testOnReturn=" + testOnReturn +
          ", testWhileIdle=" + testWhileIdle +
          '}';
    }
}
