package org.ciena.onosproject.cluster.etcd.impl;

public class ClusterMetadataReadFailureException extends ClusterMetadataException {

    private static final long serialVersionUID = -5743193140032227371L;

    private static final String MESSAGE = "unable to read the cluster metadata for '%s': detail '%s'";

    public ClusterMetadataReadFailureException(Type type, String data, Throwable cause) {
        super(String.format(MESSAGE, typeToString(type), data), cause);
    }

    public ClusterMetadataReadFailureException(Type type, String data) {
        super(String.format(MESSAGE, typeToString(type), data), null);
    }
}
