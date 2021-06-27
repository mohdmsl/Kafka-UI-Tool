package org.example.topic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Broker {
    AdminClient adminClient;

    public Broker(AdminClient adminClient) {
        this.adminClient = adminClient;
    }


    public List<String> getBrokers() {
        List<Node> nodes = getClusterInfo();
        List<String> hosts = new ArrayList<>();
        if (nodes != null) {
            for (Node node : nodes) {
                hosts.add(node.host() + ":" + node.port());
            }
        }
        return hosts;
    }

    private List<Node> getClusterInfo() {
        DescribeClusterResult describeClusterResult = this.adminClient.describeCluster();
        List<Node> nodes = null;
        try {
            nodes = new ArrayList<>(describeClusterResult.nodes().get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return nodes;
    }

    public int getNumberOfBrokers() {
        return getClusterInfo().size();
    }
}
