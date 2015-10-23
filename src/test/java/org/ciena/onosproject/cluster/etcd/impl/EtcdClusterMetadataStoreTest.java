package org.ciena.onosproject.cluster.etcd.impl;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.boon.etcd.ClientBuilder;
import org.boon.etcd.Etcd;
import org.boon.etcd.Response;
import org.junit.Test;
import org.onlab.packet.IpAddress;
import org.onosproject.cluster.ClusterMetadata;
import org.onosproject.cluster.ControllerNode;
import org.onosproject.cluster.DefaultControllerNode;
import org.onosproject.cluster.NodeId;
import org.onosproject.cluster.Partition;
import org.onosproject.store.service.Versioned;
import org.vertx.java.core.json.JsonArray;

public class EtcdClusterMetadataStoreTest {

    private static final String CONNECTION = "http://127.0.0.1:4001";
    private static final String CLUSTER_NAME = "default";
    private static final String CLUSTER_CONFIG_KEY = "onos/cluster/%s/config";
    private static final String CLUSTER_OPERATIONAL_KEY = "onos/cluster/%s/operational/%s";
    private static final String CLUSTER_OPERATIONAL_DIR_KEY = "onos/cluster/%s/operational";

    private ClusterMetadata buildMetadata(int replicaSize, String name, String... ips) {
        ClusterMetadata.Builder md = ClusterMetadata.builder();

        md.withName(name);

        List<ControllerNode> nodes = new ArrayList<ControllerNode>();
        int idx = 1;
        for (String ip : ips) {
            nodes.add(new DefaultControllerNode(new NodeId(ip), IpAddress.valueOf(ip), 9000 + idx));
            idx += 1;
        }
        md.withControllerNodes(nodes);

        List<Partition> parts = new ArrayList<Partition>();
        // p0 has everyone
        List<NodeId> members = new ArrayList<NodeId>();
        for (String ip : ips) {
            members.add(new NodeId(ip));
        }
        parts.add(new Partition("p0", members));

        int cnt = (replicaSize < 1 ? 3 : replicaSize);
        for (int i = 0; i < ips.length; i++) {
            members = new ArrayList<NodeId>();
            for (int j = 0; j < cnt; j++) {
                members.add(new NodeId(ips[(i + j) % ips.length]));
            }
            parts.add(new Partition("p" + (i + 1), members));
        }
        md.withPartitions(parts);

        return md.build();
    }

    private ClusterMetadata buildMetadata(String name, String... ips) {
        return buildMetadata(0, name, ips);
    }

    private void eraseMetaData() {
        Etcd etcd = ClientBuilder.builder().hosts(URI.create(CONNECTION)).createClient();

        for (int retry = 3; retry > 0; retry--) {
            try {
                etcd.delete(String.format(CLUSTER_CONFIG_KEY, CLUSTER_NAME));
                break;
            } catch (Exception e) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e1) {
                }
            }
        }
        for (int retry = 3; retry > 0; retry--) {
            try {
                etcd.deleteDir(String.format(CLUSTER_OPERATIONAL_DIR_KEY, CLUSTER_NAME));
                break;
            } catch (Exception e) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e1) {
                }
            }
        }
    }

    private void pushActiveReplicas(ClusterMetadata md) {
        Etcd etcd = ClientBuilder.builder().hosts(URI.create(CONNECTION)).createClient();
        for (Partition part : md.getPartitions()) {
            JsonArray push = new JsonArray();
            for (NodeId member : part.getMembers()) {
                push.add(member.toString());
            }
            for (int retry = 3; retry > 0; retry--) {
                Response response = etcd.set(String.format(CLUSTER_OPERATIONAL_KEY, CLUSTER_NAME, part.getName()),
                        push.encode());
                if (response.successful()) {
                    break;
                }
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    @Test
    public void readWriteBackTest() {
        eraseMetaData();
        ClusterMetadata prime = buildMetadata("default", "1.2.3.4", "5.6.7.8", "9.10.11.12");
        EtcdClusterMetadataStore store = new EtcdClusterMetadataStore(CONNECTION, CLUSTER_NAME);
        store.activate();

        store.setClusterMetadata(prime);
        Versioned<ClusterMetadata> back = store.getClusterMetadata();

        assertEquals("read back is different than what was wrote", prime, back.value());

        store.deactivate();
    }

    private void validateReplicas(Collection<NodeId> replicas, String... expected) {
        assertEquals("incorrent number of active replicas", expected.length, replicas.size());

        for (String replica : expected) {
            replicas.remove(new NodeId(replica));
        }
        assertEquals("replica list not what expected", 0, replicas.size());
    }

    @Test
    public void getPartitionReplicasTest() {
        eraseMetaData();
        ClusterMetadata prime = buildMetadata("default", "1.2.3.4", "5.6.7.8", "9.10.11.12");
        pushActiveReplicas(prime);

        EtcdClusterMetadataStore store = new EtcdClusterMetadataStore(CONNECTION, CLUSTER_NAME);
        store.activate();
        store.setClusterMetadata(prime);

        Collection<NodeId> replicas = store.getActiveReplicas("p0");
        validateReplicas(replicas, "1.2.3.4", "5.6.7.8", "9.10.11.12");

        store.deactivate();
    }

    @Test
    public void modifyUnsetPartitionReplicasTest() {
        eraseMetaData();
        ClusterMetadata prime = buildMetadata("default", "1.2.3.4", "5.6.7.8", "9.10.11.12");
        pushActiveReplicas(prime);

        EtcdClusterMetadataStore store = new EtcdClusterMetadataStore(CONNECTION, CLUSTER_NAME);
        store.activate();

        store.setClusterMetadata(prime);

        store.unsetActiveReplica("p1", new NodeId("5.6.7.8"));
        Collection<NodeId> replicas = store.getActiveReplicas("p1");
        validateReplicas(replicas, "1.2.3.4", "9.10.11.12");

        store.deactivate();
    }

    @Test
    public void modifyUnsetUnknownReplicaTest() {
        eraseMetaData();
        ClusterMetadata prime = buildMetadata("default", "1.2.3.4", "5.6.7.8", "9.10.11.12");
        pushActiveReplicas(prime);

        EtcdClusterMetadataStore store = new EtcdClusterMetadataStore(CONNECTION, CLUSTER_NAME);
        store.activate();
        store.setClusterMetadata(prime);

        store.unsetActiveReplica("p1", new NodeId("2.4.6.8"));
        Collection<NodeId> replicas = store.getActiveReplicas("p1");
        validateReplicas(replicas, "1.2.3.4", "5.6.7.8", "9.10.11.12");

        store.deactivate();
    }

    @Test
    public void modifySetReplicaTest() {
        eraseMetaData();
        ClusterMetadata prime = buildMetadata(2, "default", "1.2.3.4", "5.6.7.8", "9.10.11.12");
        pushActiveReplicas(prime);

        EtcdClusterMetadataStore store = new EtcdClusterMetadataStore(CONNECTION, CLUSTER_NAME);
        store.activate();
        store.setClusterMetadata(prime);

        store.setActiveReplica("p1", new NodeId("9.10.11.12"));
        Collection<NodeId> replicas = store.getActiveReplicas("p1");
        validateReplicas(replicas, "1.2.3.4", "5.6.7.8", "9.10.11.12");

        store.deactivate();
    }

    @Test
    public void modifySetDuplicateReplicaTest() {
        eraseMetaData();
        ClusterMetadata prime = buildMetadata("default", "1.2.3.4", "5.6.7.8", "9.10.11.12");
        pushActiveReplicas(prime);

        EtcdClusterMetadataStore store = new EtcdClusterMetadataStore(CONNECTION, CLUSTER_NAME);
        store.activate();
        store.setClusterMetadata(prime);

        store.setActiveReplica("p1", new NodeId("1.2.3.4"));
        Collection<NodeId> replicas = store.getActiveReplicas("p1");
        validateReplicas(replicas, "1.2.3.4", "5.6.7.8", "9.10.11.12");

        store.deactivate();
    }
}
