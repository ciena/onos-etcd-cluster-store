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
