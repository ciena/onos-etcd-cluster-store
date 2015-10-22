package org.ciena.onosproject.cluster.etcd.impl;

public class ClusterMetadataWriteFailureException extends ClusterMetadataException {

    private static final long serialVersionUID = -5431401589593426109L;

    private static final String MESSAGE = "unable to update the cluster metadata for '%s': detail '%s'";

    public ClusterMetadataWriteFailureException(Type type, String data, Throwable cause) {
        super(String.format(MESSAGE, typeToString(type), data), cause);
    }

    public ClusterMetadataWriteFailureException(Type type, String data) {
        super(String.format(MESSAGE, typeToString(type), data), null);
    }
}
