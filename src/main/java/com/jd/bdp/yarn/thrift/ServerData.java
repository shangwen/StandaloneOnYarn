package com.jd.bdp.yarn.thrift;

/**
 * Created by tangshangwen on 17-3-15.
 */
public class ServerData implements ServerInfo{

    private String hostAddress;
    private int port;

    public ServerData() {

    }

    public ServerData(String hostAddress, int port) {
        this.hostAddress = hostAddress;
        this.port = port;
    }

    public ServerData setHostAddress(String hostAddress) {
        this.hostAddress = hostAddress;
        return this;
    }

    public ServerData setPort(int port) {
        this.port = port;
        return this;
    }

    @Override
    public String toString() {
        return "ServerData{" +
                "hostAddress='" + hostAddress + '\'' +
                ", port=" + port +
                '}';
    }

    @Override
    public String getServerAddress() {
        return hostAddress;
    }

    @Override
    public int getServerPort() {
        return port;
    }
}
