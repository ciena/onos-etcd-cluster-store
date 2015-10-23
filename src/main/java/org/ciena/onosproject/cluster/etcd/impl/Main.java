package org.ciena.onosproject.cluster.etcd.impl;

import org.onosproject.cluster.ClusterMetadata;
import org.onosproject.store.service.Versioned;

public final class Main {
    private Main() {
    }

    public static final void main(String[] argv) {
        EtcdClusterMetadataStore ds = new EtcdClusterMetadataStore(null, "default");
        ds.activate();

        Versioned<ClusterMetadata> md = ds.getClusterMetadata();

        try {
            Object o = new Object();
            synchronized (o) {
                o.wait();
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
