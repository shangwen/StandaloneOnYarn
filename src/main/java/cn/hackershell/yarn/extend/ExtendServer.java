package cn.hackershell.yarn.extend;

import cn.hackershell.yarn.thrift.ServerInfo;

/**
 * Created by tangshangwen on 17-3-17.
 *
 * 注册Master地址到外部系统(HDFS,或Web)
 */
public interface ExtendServer {
    public ServerInfo FindServerData();

    public void registerServerData(ServerInfo server);
}
