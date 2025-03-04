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
import redis.clients.jedis.HostAndPort;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * Configuration for Jedis cluster.
 */
public class FlinkJedisClusterConfig extends FlinkJedisConfigBase {
    private static final long serialVersionUID = 1L;

    private final Set<InetSocketAddress> nodes;
    private final int maxRedirections;


    /**
     * Jedis cluster configuration.
     * The list of node is mandatory, and when nodes is not set, it throws NullPointerException.
     *
     * @param nodes list of node information for JedisCluster
     * @param connectionTimeout socket / connection timeout. The default is 2000
     * @param maxRedirections limit of redirections-how much we'll follow MOVED or ASK
     * @param maxTotal the maximum number of objects that can be allocated by the pool
     * @param maxIdle the cap on the number of "idle" instances in the pool
     * @param minIdle the minimum number of idle objects to maintain in the pool
     * @param password the password of redis cluster
     * @param testOnBorrow Whether objects borrowed from the pool will be validated before being returned, default value is false
     * @param testOnReturn Whether objects borrowed from the pool will be validated when they are returned to the pool, default value is false
     * @param testWhileIdle Whether objects sitting idle in the pool will be validated by the idle object evictor, default value is false
     * @throws NullPointerException if parameter {@code nodes} is {@code null}
     */
    private FlinkJedisClusterConfig(Set<InetSocketAddress> nodes, int connectionTimeout, int maxRedirections,
                                    int maxTotal, int maxIdle, int minIdle, String password,
                                    boolean testOnBorrow, boolean testOnReturn, boolean testWhileIdle) {
        super(connectionTimeout, maxTotal, maxIdle, minIdle, password, testOnBorrow, testOnReturn, testWhileIdle);

        Preconditions.checkNotNull(nodes, "Node information should be presented");
        Preconditions.checkArgument(!nodes.isEmpty(), "Redis cluster hosts should not be empty");
        this.nodes = new HashSet<>(nodes);
        this.maxRedirections = maxRedirections;
    }



    /**
     * Returns nodes.
     *
     * @return list of node information
     */
    public Set<HostAndPort> getNodes() {
        Set<HostAndPort> ret = new HashSet<>();
        for (InetSocketAddress node : nodes) {
            ret.add(new HostAndPort(node.getHostName(), node.getPort()));
        }
        return ret;
    }

    /**
     * Returns limit of redirection.
     *
     * @return limit of redirection
     */
    public int getMaxRedirections() {
        return maxRedirections;
    }


    /**
     * Builder for initializing  {@link FlinkJedisClusterConfig}.
     */
    public static class Builder {
        private Set<InetSocketAddress> nodes;
        private int timeout = RedisOptions.CONNECTION_TIMEOUT_MS.defaultValue();
        private int maxRedirections = RedisOptions.CONNECTION_MAX_REDIRECTIONS.defaultValue();
        private int maxTotal = RedisOptions.CONNECTION_MAX_TOTAL.defaultValue();
        private int maxIdle = RedisOptions.CONNECTION_MAX_IDLE.defaultValue();
        private int minIdle = RedisOptions.CONNECTION_MIN_IDLE.defaultValue();
        private boolean testOnBorrow = RedisOptions.CONNECTION_TEST_ON_BORROW.defaultValue();
        private boolean testOnReturn = RedisOptions.CONNECTION_TEST_ON_RETURN.defaultValue();
        private boolean testWhileIdle = RedisOptions.CONNECTION_TEST_WHILE_IDLE.defaultValue();
        private String password;

        /**
         * Sets list of node.
         *
         * @param nodes list of node
         * @return Builder itself
         */
        public Builder setNodes(Set<InetSocketAddress> nodes) {
            this.nodes = nodes;
            return this;
        }

        /**
         * Sets socket / connection timeout.
         *
         * @param timeout socket / connection timeout, default value is 2000
         * @return Builder itself
         */
        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Sets limit of redirection.
         *
         * @param maxRedirections limit of redirection, default value is 5
         * @return Builder itself
         */
        public Builder setMaxRedirections(int maxRedirections) {
            this.maxRedirections = maxRedirections;
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
         * Sets value for the {@code password} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param password the password for accessing redis cluster
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
         * Builds JedisClusterConfig.
         *
         * @return JedisClusterConfig
         */
        public FlinkJedisClusterConfig build() {
            return new FlinkJedisClusterConfig(nodes, timeout, maxRedirections, maxTotal, maxIdle, minIdle, password, testOnBorrow, testOnReturn, testWhileIdle);
        }
    }

    @Override
    public String toString() {
        return "FlinkJedisClusterConfig{" +
          "nodes=" + nodes +
          ", maxRedirections=" + maxRedirections +
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
