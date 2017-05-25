package cn.hackershell.yarn.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

/**
 * Created by tangshangwen on 17-3-16.
 */
public class ContextFactory {

    /**
     * 这里可以通过配置类转换不同的launch脚本
     *
     * @param conf
     * @return
     * @throws Exception
     */
    public static ContainerLaunchContext createLanchContext(Configuration conf)
            throws Exception {

        return new SparkLaunch(conf).getLanchContext();
    }
}
