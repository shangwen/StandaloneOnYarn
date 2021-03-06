package cn.hackershell.yarn.master;

import cn.hackershell.yarn.extend.ExtendServer;
import cn.hackershell.yarn.extend.ExtendServerFactory;
import cn.hackershell.yarn.generated.AmServer;
import cn.hackershell.yarn.thrift.ServerData;
import cn.hackershell.yarn.thrift.ThriftServer;
import cn.hackershell.yarn.utils.Config;
import cn.hackershell.yarn.utils.ToolUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Created by tangshangwen on 17-3-14.
 */
public class ThriftMaster {
    private static final Log LOG = LogFactory.getLog(ThriftMaster.class);

    private ThriftServer server;
    private ServerData serverData;
    private Configuration conf;

    public ThriftMaster(int numWorker) throws Exception {
        this.conf = ToolUtils.loadProp2Conf(Config.create4Env(),
                new Configuration());
        server = new ThriftServer(conf);
        server.start();
        Thread.sleep(10000);

        serverData = server.getServerData();
        ExtendServer es = ExtendServerFactory.create(conf);
        es.registerServerData(serverData);

        TTransport transport = new TSocket(serverData.getServerAddress(),
                serverData.getServerPort());
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);

        AmServer.Client client = new AmServer.Client(protocol);
        client.addWorkers(numWorker);
    }

    public static void main(String[] args) {
        LOG.info("Starting... ThriftMaster");
        try {
            new ThriftMaster(2);
        } catch (Exception e) {
          LOG.info("start ThriftMaster Failed ", e);
        }

    }
}
