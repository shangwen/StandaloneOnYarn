package cn.hackershell.yarn.client;

import cn.hackershell.yarn.utils.StandaloneOnYarnCommonKeys;
import cn.hackershell.yarn.utils.ToolUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tangshangwen on 17-3-17.
 */
public class SubmitClient {
    private static final Log LOG = LogFactory.getLog(SubmitClient.class);

    private Configuration conf;
    private YarnClient yarnClient;
    private YarnClientApplication application;
    private ApplicationSubmissionContext context;
    private String appName;
    private String queueName;
    private ContainerLaunchContext amContainer;
    private String applicationType;
    private String amtar;

    public SubmitClient(Map<String, String> config) {
        this.conf = ToolUtils.loadProp2Conf(config,
                new YarnConfiguration());

        this.yarnClient = YarnClient.createYarnClient();
        this.yarnClient.init(conf);
        this.yarnClient.start();
        this.applicationType = conf.get(
                StandaloneOnYarnCommonKeys.YARN_STANDALONE_APP_TYPE);
        this.appName = conf.get(
                StandaloneOnYarnCommonKeys.YARN_STANDALONE_APP_NAME);
        this.queueName = conf.get(
                StandaloneOnYarnCommonKeys.YARN_STANDALONE_QUEUE_NAME);
        this.amtar = conf.get(StandaloneOnYarnCommonKeys.YARN_STANDALONE_HDFS_PATH)
                + conf.get(StandaloneOnYarnCommonKeys.YARN_STANDALONE_AM_TAR);
    }

    public void submitApplication() throws YarnException, IOException {
        application = yarnClient.createApplication();
        context = application.getApplicationSubmissionContext();

        context.setApplicationName(appName);
        context.setQueue(queueName);
        context.setApplicationType(applicationType);

        LocalResource appMasterResource = ToolUtils.createResource(this.conf, amtar);
        amContainer = Records.newRecord(ContainerLaunchContext.class);

        Map<String, String> appMasterEnv = new HashMap<>();
        setupAppMasterEnv(appMasterEnv);

        amContainer.setEnvironment(appMasterEnv);
        amContainer.setLocalResources(
                Collections.singletonMap("standalone-yarn.zip", appMasterResource));
        // 添加命令, 启动AM程序,日志输出到yarn's log
        List cmdList = Collections.singletonList(
                "unzip standalone-yarn.zip;" +
                "export STANDALONE_ON_YARN_HOME=`pwd`;" +
                        "$JAVA_HOME/bin/java" +
                        " -Xmx2048M" +
                        " ThriftMaster " +
                        "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout " +
                        "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
        );
        amContainer.setCommands(cmdList);
        LOG.info("cmd - " + cmdList);
        context.setAMContainerSpec(amContainer);
        context.setResource(
                Resource.newInstance(StandaloneOnYarnCommonKeys.YARN_AM_MEN_DEFAULT * 1024,
                        StandaloneOnYarnCommonKeys.YARN_AM_CPU_DEFAULT));
        context.setPriority(ToolUtils.getPriority(StandaloneOnYarnCommonKeys.YARN_AM_PRIORITY));

        yarnClient.submitApplication(context);
        ApplicationId appId = application.getApplicationSubmissionContext().getApplicationId();
        waitTermination(appId);
    }

    /**
     * 判断客户端是否判断退出
     *
     * @param appState
     * @return
     */
    private boolean isContinue(YarnApplicationState appState) {
        return appState != YarnApplicationState.FINISHED &&
                appState != YarnApplicationState.KILLED &&
                appState != YarnApplicationState.FAILED;
    }

    private void waitTermination(ApplicationId appId) {
        try {
            ApplicationReport appReport = yarnClient.getApplicationReport(appId);
            YarnApplicationState appState = appReport.getYarnApplicationState();
            while (isContinue(appState)) {
                Thread.sleep(5000);
                appReport = yarnClient.getApplicationReport(appId);
                appState = appReport.getYarnApplicationState();
                LOG.info("App " + appId + " status is " + appState);
            }
        } catch (Exception e) {
            LOG.error("waitTermination Error ", e);
        }
    }

    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        // TODO 加载依赖jar包, 待优化
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
                    c.trim());
        }
        Apps.addToEnvironment(appMasterEnv,
                ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator + "*");
    }
}
