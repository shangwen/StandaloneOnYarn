package com.jd.bdp.yarn.utils;

import com.jd.bdp.yarn.generated.AmServer;
import com.jd.bdp.yarn.thrift.ServerData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by tangshangwen on 17-3-15.
 */
public class NetworkUtils {

    private static final Log LOG = LogFactory.getLog(NetworkUtils.class);

    private static void bindPort(String host, int port) throws Exception {
        Socket socket = new Socket();
        socket.bind(new InetSocketAddress(host, port));
        socket.close();
    }

    /**
     * 判断网络端口是否可用
     *
     * @param port
     * @return
     */
    public static boolean isPortAvailable(int port) {
        try {
            bindPort("0.0.0.0", port);
            bindPort(InetAddress.getLocalHost().getHostAddress(), port);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static int getSocketPort() throws IOException {
        ServerSocket ss =  new ServerSocket(0);
        int port = ss.getLocalPort();
        LOG.info("Get port:" + port);
        ss.close();
        return port;
    }

    public static AmServer.Client createThriftClient(ServerData serverData)
            throws Exception {
        TTransport transport = new TSocket(serverData.getServerAddress(), serverData.getServerPort());
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        AmServer.Client client = new AmServer.Client(protocol);
        return client;
    }
}
