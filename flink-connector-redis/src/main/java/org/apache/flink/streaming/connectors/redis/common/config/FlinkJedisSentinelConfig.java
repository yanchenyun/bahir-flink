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

import java.util.HashSet;
import java.util.Set;

/**
 * Configuration for Jedis Sentinel pool.
 */
public class FlinkJedisSentinelConfig extends FlinkJedisConfigBase {
    private static final long serialVersionUID = 1L;

    private final String masterName;
    private final Set<String> sentinels;
    private final int soTimeout;
    private final int database;

    /**
     * Jedis Sentinels config.
     * The master name and sentinels are mandatory, and when you didn't set these, it throws NullPointerException.
     *
     * @param masterName master name of the replica set
     * @param sentinels set of sentinel hosts
     * @param connectionTimeout timeout connection timeout
     * @param soTimeout timeout socket timeout
     * @param password password, if any
     * @param database database database index
     * @param maxTotal maxTotal the maximum number of objects that can be allocated by the pool
     * @param maxIdle the cap on the number of "idle" instances in the pool
     * @param minIdle the minimum number of idle objects to maintain in the pool
     * @param testOnBorrow Whether objects borrowed from the pool will be validated before being returned, default value is false
     * @param testOnReturn Whether objects borrowed from the pool will be validated when they are returned to the pool, default value is false
     * @param testWhileIdle Whether objects sitting idle in the pool will be validated by the idle object evictor, default value is false
     * @throws NullPointerException if {@code masterName} or {@code sentinels} is {@code null}
     * @throws IllegalArgumentException if {@code sentinels} are empty
     */
    private FlinkJedisSentinelConfig(String masterName, Set<String> sentinels,
                                     int connectionTimeout, int soTimeout,
                                     String password, int database,
                                     int maxTotal, int maxIdle, int minIdle,
                                     boolean testOnBorrow, boolean testOnReturn, boolean testWhileIdle) {
        super(connectionTimeout, maxTotal, maxIdle, minIdle, password, testOnBorrow, testOnReturn, testWhileIdle);

        Preconditions.checkNotNull(masterName, "Master name should be presented");
        Preconditions.checkNotNull(sentinels, "Sentinels information should be presented");
        Preconditions.checkArgument(!sentinels.isEmpty(), "Sentinel hosts should not be empty");

        this.masterName = masterName;
        this.sentinels = new HashSet<>(sentinels);
        this.soTimeout = soTimeout;
        this.database = database;
    }

    /**
     * Returns master name of the replica set.
     *
     * @return master name of the replica set.
     */
    public String getMasterName() {
        return masterName;
    }

    /**
     * Returns Sentinels host addresses.
     *
     * @return Set of Sentinels host addresses
     */
    public Set<String> getSentinels() {
        return sentinels;
    }

    /**
     * Returns socket timeout.
     *
     * @return socket timeout
     */
    public int getSoTimeout() {
        return soTimeout;
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
     * Builder for initializing {@link FlinkJedisSentinelConfig}.
     */
    public static class Builder {
        private String masterName;
        private Set<String> sentinels;
        private int connectionTimeout = RedisOptions.CONNECTION_TIMEOUT_MS.defaultValue();
        private int soTimeout = RedisOptions.CONNECTION_TIMEOUT_MS.defaultValue();
        private String password;
        private int database = RedisOptions.DB_NUM.defaultValue();
        private int maxTotal = RedisOptions.CONNECTION_MAX_TOTAL.defaultValue();
        private int maxIdle = RedisOptions.CONNECTION_MAX_IDLE.defaultValue();
        private int minIdle = RedisOptions.CONNECTION_MIN_IDLE.defaultValue();
        private boolean testOnBorrow = RedisOptions.CONNECTION_TEST_ON_BORROW.defaultValue();
        private boolean testOnReturn = RedisOptions.CONNECTION_TEST_ON_RETURN.defaultValue();
        private boolean testWhileIdle = RedisOptions.CONNECTION_TEST_WHILE_IDLE.defaultValue();

        /**
         * Sets master name of the replica set.
         *
         * @param masterName  master name of the replica set
         * @return Builder itself
         */
        public Builder setMasterName(String masterName) {
            this.masterName = masterName;
            return this;
        }

        /**
         * Sets sentinels address.
         *
         * @param sentinels host set of the sentinels
         * @return Builder itself
         */
        public Builder setSentinels(Set<String> sentinels) {
            this.sentinels = sentinels;
            return this;
        }

        /**
         * Sets connection timeout.
         *
         * @param connectionTimeout connection timeout, default value is 2000
         * @return Builder itself
         */
        public Builder setConnectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Sets socket timeout.
         *
         * @param soTimeout socket timeout, default value is 2000
         * @return Builder itself
         */
        public Builder setSoTimeout(int soTimeout) {
            this.soTimeout = soTimeout;
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
         * Builds JedisSentinelConfig.
         *
         * @return JedisSentinelConfig
         */
        public FlinkJedisSentinelConfig build(){
            return new FlinkJedisSentinelConfig(masterName, sentinels, connectionTimeout, soTimeout,
                password, database, maxTotal, maxIdle, minIdle, testOnBorrow, testOnReturn, testWhileIdle);
        }
    }

    @Override
    public String toString() {
        return "FlinkJedisSentinelConfig{" +
          "masterName=" + masterName +
          ", sentinels=" + sentinels +
          ", soTimeout=" + soTimeout +
          ", database=" + database +
          ", maxTotal=" + maxTotal +
          ", maxIdle=" + maxIdle +
          ", minIdle=" + minIdle +
          ", connectionTimeout=" + connectionTimeout +
          ", password=" + password +
          ", testOnBorrow=" + testOnBorrow +
          ", testOnReturn=" + testOnReturn +
          ", testWhileIdle=" + testWhileIdle +
          '}';
    }
}
