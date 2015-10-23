/*
 * Copyright 2015 Ciena Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ciena.onosproject.cluster.etcd.impl;

public class ClusterMetadataException extends Exception {

    private static final long serialVersionUID = -6662435680187583409L;


    enum Type {
        Partition, Node, Cluster
    }

    protected static final String PARTITION = "partition";
    protected static final String NODE = "node";
    protected static final String CLUSTER = "cluster";
    protected static final String UNKNOWN = "unknown";

    public static final String typeToString(Type type) {
        switch (type) {
        case Partition:
            return PARTITION;
        case Node:
            return NODE;
        case Cluster:
            return CLUSTER;
        default:
            return UNKNOWN;
        }
    }

    public ClusterMetadataException(String message, Throwable cause) {
        super(message, cause);
    }
}
