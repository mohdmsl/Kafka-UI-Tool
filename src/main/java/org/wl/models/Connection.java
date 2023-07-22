package org.wl.models;

public class Connection {
    private String id;
    private String name;
    private String zookeeperHost;
    private String zookeeperPort;
    private String bootstrapServer;

    public Connection(String id, String name, String zookeeperHost, String zookeeperPort, String bootstrapServer) {
        this.id = id;
        this.name = name;
        this.zookeeperHost = zookeeperHost;
        this.zookeeperPort = zookeeperPort;
        this.bootstrapServer = bootstrapServer;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getZookeeperHost() {
        return zookeeperHost;
    }

    public void setZookeeperHost(String zookeeperHost) {
        this.zookeeperHost = zookeeperHost;
    }

    public String getZookeeperPort() {
        return zookeeperPort;
    }

    public void setZookeeperPort(String zookeeperPort) {
        this.zookeeperPort = zookeeperPort;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }
}
