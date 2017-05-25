namespace java com.jd.bdp.yarn.generated

service AmServer{
    //预备启动多少worker
    void addWorkers(1:i32 number);
    //保留多少worker
    void reserveWorkers(1:i32 number);
    //获取当前worker数
    i32 getWorkersNum();
    //获取当前应起到的worker数
    i32 currentContainerNum();
    //获取当前应起到的workers明细信息
    string getContainerInfo();

    void startAmServer();
    void stopAmServer();
    //启动所有的预备启动的workers
    void startWorkers();
    //关闭所有的预备关闭的workers
    void stopWorkers();
    //关闭MasterServer 和 Workers
    void shutdown();
    //返回所有worker的信息
    map<string,string> getAllWorkers();
    //设置参数
    void setAmServerConf(1:map<string,string> conf);

    void checkContainerState();
}
