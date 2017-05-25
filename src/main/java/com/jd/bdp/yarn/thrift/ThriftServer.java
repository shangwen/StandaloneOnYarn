package com.jd.bdp.yarn.thrift;

import com.jd.bdp.yarn.generated.AmServer;
import com.jd.bdp.yarn.utils.NetworkUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by tangshangwen on 17-3-15.
 */
public class ThriftServer implements Server {
    private static final Log LOG = LogFactory.getLog(ThriftServer.class);

    private ServerData serverData;
    private Configuration conf;
    private ExecutorService es;

    public ThriftServer(Configuration conf) {
        LOG.info("ThriftServer init...");
        this.conf = conf;
        this.es = Executors.newSingleThreadExecutor();
        this.serverData = new ServerData();
    }

    @Override
    public void start() {
        LOG.info("ThriftServer start...");
        this.es.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    int port = NetworkUtils.getSocketPort();
                    serverData
                            .setHostAddress(InetAddress.getLocalHost().getHostAddress())
                            .setPort(port);
                    LOG.info("serverData=" + serverData);

                    AmServer.Processor processor = new AmServer.Processor(new ThriftServerHandler(conf));
                    TThreadPoolServer.Args args = new TThreadPoolServer.Args(new TServerSocket(port, 60000));
                    args.processor(processor);
                    args.protocolFactory(new TBinaryProtocol.Factory());

                    TServer server = new TThreadPoolServer(args);
                    LOG.info("Started ThriftServer ...");
                    server.serve();
                } catch (Exception e) {
                    LOG.info("ThriftServer start failed, cause ", e);
                }
            }
        });

    }

    public ServerData getServerData() {
        return serverData;
    }

    @Override
    public void stop() {
        this.es.shutdown();
    }
}
