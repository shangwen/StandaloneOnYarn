package com.jd.bdp.yarn.utils;

/**
 * Created by tangshangwen on 17-3-14.
 */
public class StandaloneOnYarnCommonKeys {
    public static final int YARN_AM_PRIORITY = 1;

    public static final String YARN_AM_CPU_KEY = "yarn.standalone.am.core";
    public static final int YARN_AM_CPU_DEFAULT = 1;

    public static final String YARN_AM_MEN_KEY = "yarn.standalone.am.mem";
    public static final int YARN_AM_MEN_DEFAULT = 2;

    public static final String YARN_STANDALONE_WORKERS_KEY = "yarn.standalone.workers";

    public static final String YARN_STANDALONE_APP_TYPE = "yarn.standalone.appType";
    public static final String YARN_STANDALONE_APP_NAME = "yarn.standalone.appName";
    public static final String YARN_STANDALONE_QUEUE_NAME = "yarn.standalone.queueName";


    public static final String YARN_STANDALONE_WORKER_RELAX_LOCALITY = "yarn.standalone.worker.relax.locality";

    public static final String YARN_STANDALONE_WORKER_CORE = "yarn.standalone.worker.core";
    public static final int YARN_STANDALONE_WORKER_CORE_DEFAULT = 8;

    public static final String YARN_STANDALONE_WORKER_MEM = "yarn.standalone.worker.mem";
    public static final int YARN_STANDALONE_WORKER_MEM_DEFAULT = 10;


    public static final String YARN_STANDALONE_WORKER_PRIORITY = "yarn.standalone.worker.priority";
    public static final int YARN_STANDALONE_WORKER_PRIORITY_DEFAULT = 1;

    public static final String YARN_STANDALONE_HDFS_PATH = "yarn.standalone.hdfs.path";
    public static final String YARN_STANDALONE_LOG_DIR = "yarn.standalone.log";
    public static final String YARN_STANDALONE_MASTER_ADDRESS = "yarn.standalone.master.address";


    public static final String YARN_STANDALONE_TAR = "yarn.standalone.tar";
    public static final String YARN_STANDALONE_TAR_DEFAULT = "hdfs://ns1/tmp/yarnStandalone/standalone.tar";

    public static final String YARN_STANDALONE_AM_TAR = "yarn.standalone.appmaster.tar";
    public static final String YARN_STANDALONE_AM_TAR_DEFAULT = "/tmp/yarnStandalone/yarn-standalone-am.tar";

    public static final String YARN_STANDALONE_EXTEND_SERVER = "yarn.standalone.extend.server";
    public static final String YARN_STANDALONE_EXTEND_SERVER_DEFAULT = "com.jd.bdp.yarn.extend.HdfsExtendServerImpl";

    public static final String YARN_STANDALONE_HDFS_EXTEND_FILE = "yarn.standalone.hdfs.extend.file";



}
