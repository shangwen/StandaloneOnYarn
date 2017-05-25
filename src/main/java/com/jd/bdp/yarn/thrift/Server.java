package com.jd.bdp.yarn.thrift;

/**
 * Created by tangshangwen on 17-3-15.
 */
public interface Server {
    /**
     * 启动服务
     */
    public void start();

    /**
     * 停止服务
     */
    public void stop();
}
