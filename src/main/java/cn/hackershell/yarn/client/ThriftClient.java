package cn.hackershell.yarn.client;

import cn.hackershell.yarn.extend.ExtendServer;
import cn.hackershell.yarn.extend.ExtendServerFactory;
import cn.hackershell.yarn.generated.AmServer;
import cn.hackershell.yarn.thrift.ServerInfo;
import cn.hackershell.yarn.utils.Config;
import cn.hackershell.yarn.utils.ToolUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by tangshangwen on 17-3-17.
 */
public class ThriftClient{
    private static final Log LOG = LogFactory.getLog(ThriftClient.class);

    private ThriftClient() {
    }

    public static AmServer.Client create() {
        ExtendServer extendServer = ExtendServerFactory.create(
                ToolUtils.loadProp2Conf(Config.create4Env(),
                new Configuration()));
        ServerInfo serverData = extendServer.FindServerData();
        TTransport transport = new TSocket(serverData.getServerAddress(),
                serverData.getServerPort());
        try {
            transport.open();
        } catch (TTransportException e) {
            LOG.error("Transport open failed ", e);
        }
        TProtocol protocol = new TBinaryProtocol(transport);
        return new AmServer.Client(protocol);
    }
}
