/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.kafka;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * A Kafka broker.
 *
 * @author unknown
 */
public class Broker implements Serializable, Comparable<Broker> {
    /**
     * The default port to use.
     */
    public static final int PORT_DEFAULT = 9092;
    /**
     * The host to connect to.
     */
    private final String host;
    /**
     * The port to connect to.
     */
    private final int port;

    /**
     * Constructs and invalid empty Broker. This constructor is only provided
     * for reflection access by frameworks like kryo.
     */
    private Broker() {
        this(null,
                0);
    }

    /**
     * Creates a new broker with specified host and port values.
     * @param hostArg the host to connect to
     * @param portArg the port to connect to
     */
    public Broker(final String hostArg, final int portArg) {
        this.host = hostArg;
        this.port = portArg;
    }

    /**
     * Creates a new broker with the specified host and {@link #PORT_DEFAULT}.
     * @param hostArg the host to connect to
     */
    public Broker(final String hostArg) {
        this(hostArg, PORT_DEFAULT);
    }

    /**
     * Gets the host to connect to.
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the port to connect to.
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * The hash code is based on {@code host} and {@code port}.
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(host, port);
    }

    /**
     * The equal comparison is based on  {@code host} and {@code port}.
     * @param obj the object to compare to
     * @return {@code true} if this broker and {@code obj} can be considered
     * equal
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Broker other = (Broker) obj;
        return Objects.equal(this.host, other.getHost())
                && Objects.equal(this.port, other.getPort());
    }

    /**
     * String the presentation of this broker.
     * @return the string representation
     */
    @Override
    public String toString() {
        return host + ":" + port;
    }

    /**
     * Parses a string to contruct a broker.
     * @param host the string in the form of {@code [host]} or
     * {@code [host]:[port]}
     * @return the constructed broker
     */
    public static Broker fromString(final String host) {
        Broker hp;
        String[] spec = host.split(":");
        if (spec.length == 1) {
            hp = new Broker(spec[0]);
        } else if (spec.length == 2) {
            hp = new Broker(spec[0], Integer.parseInt(spec[1]));
        } else {
            throw new IllegalArgumentException("Invalid host specification: "
                    + host);
        }
        return hp;
    }

    /**
     * Compares this broker to {@code other}.
     * @param other the broker to compare to
     * @return {@code 0} if both port and host are equals or a value less than
     * {@code 0} or more than {@code 0} after applying the logic of
     * {@link Integer#compare(int, int) } and
     * {@link String#compareTo(java.lang.String) } on {@code port} and
     * {@code host}
     */
    @Override
    public int compareTo(final Broker other) {
        if (this.host.equals(other.getHost())) {
            return this.port - other.getPort();
        } else {
            return this.host.compareTo(other.getHost());
        }
    }
}
