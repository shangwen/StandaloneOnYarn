package cn.hackershell.yarn.extend;

import cn.hackershell.yarn.thrift.ServerData;
import cn.hackershell.yarn.thrift.ServerInfo;
import cn.hackershell.yarn.utils.StandaloneOnYarnCommonKeys;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

/**
 * Created by tangshangwen on 17-3-17.
 */
public class HdfsExtendServerImpl implements ExtendServer {
    private static final Log LOG = LogFactory.getLog(HdfsExtendServerImpl.class);
    private Configuration conf;
    private Path registerData;

    public HdfsExtendServerImpl(Configuration conf) {
        this.conf = conf;
        this.registerData = new Path(
                conf.get(StandaloneOnYarnCommonKeys.YARN_STANDALONE_HDFS_EXTEND_FILE));
    }

    @Override
    public ServerInfo FindServerData() {
        FileSystem fs = null;
        FSDataInputStream in = null;
        ServerData server = null;
        try {
            fs = registerData.getFileSystem(conf);
            if (fs.exists(registerData)) {
                FileStatus stat = fs.getFileStatus(registerData);
                in = fs.open(registerData);
                byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
                in.readFully(0, buffer);
                String str = new String(buffer);
                LOG.info("serverData=" + str);
                String[] arr = str.split(":");
                server = new ServerData(arr[0], Integer.parseInt(arr[1]));
                LOG.info("server=" + server +
                        " arr[0]=" + arr[0] +
                        " arr[1]=" + arr[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                    e.printStackTrace();
            }
        }
        return server;
    }

    @Override
    public void registerServerData(ServerInfo server) {
        LOG.info("registerServerData " + server);
        FileSystem fs = null;
        try {
            fs = registerData.getFileSystem(conf);
            if (fs.exists(registerData)) {
                LOG.error("File " + registerData.getName() + " already exists!");
                LOG.error("File " + registerData.getName() +
                        " delete success=" + fs.delete(registerData, false));
            }
            FSDataOutputStream outputStream = fs.create(registerData);
            String content = server.getServerAddress() + ":" + server.getServerPort();
            outputStream.write(content.getBytes());
            outputStream.close();
            System.out.println("create file " + registerData.toUri() + " success!");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
