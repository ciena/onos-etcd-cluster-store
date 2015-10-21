# ONOS Etcd Backed Cluster Metadata Store [![](https://img.shields.io/badge/onos-component-red.svg)]() [![](https://img.shields.io/badge/status-development-blue.svg)]()

In the 1.4 (Emu) release of the [OpenNetwork Operating System (ONOS)](http://onosproject.org) dynamic clustering
capabilities are being added, where cluster size and membership can be modified without the need to restart controller
instances and reform clusters based on new "static" configuration. to enable this capability ONOS introduced the
concept of a cluster metadata store that is used to maintain information about which nodes are in a cluster as well
as how data is partitioned across the cluster. The default implementation of the cluster metadata store is based on
the ONOS internal distributed collection classes, but the design / implementation of the capability is meant to
support alternate implementations.

### Etcd Based Implementation
This project [ciena/etcd-cluster-store](http://github.com/ciena/etcd-cluster-store) is an open source implementation,
contributed by [Ciena BluePlanet](http://www.blueplanet.com/), of an ONOS cluster metadata store using
[CoreOS's Etcd](https://coreos.com/etcd/) highly-available key value store for shared configuration and service
discovery.

### Details
The implementation is fairly straight forward, the cluster metadata is stored in etcd as a *stringified* JSON object. As ONOS supports the concept of multiple clusters, there is a key for each cluster in etcd. The keys are of the form `onos/cluster/<key-name>`.

#### Updating Cluster Information
Cluster metadata information can be updated by updating the information in etcd. The ONOS etcd implementation of the cluster metadata store is watching the key for which a instance is a member and will update the internal ONOS information as members are added / removed.

#### Examples

The format of the JSON object container the cluster information in etcd is:

    {
        "nodes" : {
            "<node-0-id>" : {
                "ip"   : "<node-0-ip>",
                "port" : <node-0-port>
            },

                    .
                    .
                    .

            "<node-n-id" : {
                "ip"   : "<node-n-ip>",
                "port" : <node-n-port>
            }
        },
        "partitions" : {
            "p0" : [
                "<node-0-id>",
                "<node-n-id>"
            ],

                    .
                    .
                    .

            "p#" : [
                "<node-X-id>",
                "<node-Y-id>",
                "<node-Z-id>"
            ]
        }
    }

An example using 3 nodes and 3 partitions as stored in etcd (*note: by default the node IP is used as the node ID*):

    {
        "nodes" : {
            "192.168.0.53" : {
                "ip"   : "192.168.0.53",
                "port" : 9876
            },
            "192.168.0.63" : {
                "ip"   : "192.168.0.63",
                "port" : 9876
            },
            "192.168.0.82" : {
                "ip"   : "192.168.0.82",
                "port" : 9876
            }
        },
        "partitions" : {
            "p0" : [
                "192.168.0.53",
                "192.168.0.63",
                "192.168.0.82"
            ],
            "p1" : [
                "192.168.0.63",
                "192.168.0.82",
                "192.168.0.53"
            ],
            "p2" : [
                "192.168.0.82",
                "192.168.0.53",
                "192.168.0.63"            
            ]
        }
    }

#### Dependencies
Currently the implementation leverates an etcd library for the interactions with the etcd servers. This library is the boon etcd-client source at https://github.com/boonproject/boon/tree/master/etcd/etcd-client. This project does not container the source to this project and the artifacts of this project are not on a public repository, as such you may have to locally build and deploy this package to compile run the etcd cluster metadata store. 
