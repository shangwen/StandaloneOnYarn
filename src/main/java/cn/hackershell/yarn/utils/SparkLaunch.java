package cn.hackershell.yarn.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.util.Records;

import java.util.Collections;
import java.util.List;

/**
 * Created by tangshangwen on 17-3-16.
 */
public class SparkLaunch extends StandaloneLaunch {

    private static final Log LOG = LogFactory.getLog(SparkLaunch.class);

    private Configuration conf;

    public SparkLaunch(Configuration conf) {
        super(conf);
        this.conf = conf;
    }

    @Override
    ContainerLaunchContext getLanchContext() throws Exception {

        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        String standalonetar = conf.get(StandaloneOnYarnCommonKeys.YARN_STANDALONE_HDFS_PATH) +
                conf.get(StandaloneOnYarnCommonKeys.YARN_STANDALONE_TAR);
        LOG.info("xx: " + conf.get(StandaloneOnYarnCommonKeys.YARN_STANDALONE_HDFS_PATH) + " yy: " + conf.get(StandaloneOnYarnCommonKeys.YARN_STANDALONE_TAR));
        int cores = conf.getInt(StandaloneOnYarnCommonKeys.YARN_STANDALONE_WORKER_CORE,
                StandaloneOnYarnCommonKeys.YARN_STANDALONE_WORKER_CORE_DEFAULT);
        int mem = conf.getInt(StandaloneOnYarnCommonKeys.YARN_STANDALONE_WORKER_MEM,
                StandaloneOnYarnCommonKeys.YARN_STANDALONE_WORKER_MEM_DEFAULT);
        String masterAddress = conf.get(StandaloneOnYarnCommonKeys.YARN_STANDALONE_MASTER_ADDRESS);

        ctx.setLocalResources(Collections.singletonMap("standalone.tar",
                ToolUtils.createResource(conf, standalonetar)));

        ctx.setEnvironment(getContainerEnv());
        String startCmd = "tar -xf standalone.tar;cd ./standalone;";
        startCmd += "./bin/spark-class org.apache.spark.deploy.worker.Worker"
                + " -c " + cores
                + " -m " + mem + "G"
                + " --webui-port 8081"
                + " -d " + ApplicationConstants.LOG_DIR_EXPANSION_VAR;
        startCmd += " " + masterAddress
                + " 1>"+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
        LOG.info("--------------------------------------------");
        LOG.info("startCMD: " + startCmd);
        LOG.info("--------------------------------------------");
        List cmdList = Collections.singletonList(startCmd);
        ctx.setCommands(cmdList);

        return ctx;
    }
}
