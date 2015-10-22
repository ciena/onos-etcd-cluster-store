package org.ciena.onosproject.cluster.etcd.impl;

import org.onosproject.cluster.ClusterMetadata;
import org.onosproject.cluster.NodeId;
import org.onosproject.store.service.Versioned;

public final class Main {
    private Main() {
    }

    public static final void main(String[] argv) {
        EtcdClusterMetadataStore ds = new EtcdClusterMetadataStore(null, "default");
        ds.activate();

        Versioned<ClusterMetadata> md = ds.getClusterMetadata();
        ds.setClusterMetadata(md.value());

        ds.setActiveReplica("p1", new NodeId("5.4.3.2"));
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
