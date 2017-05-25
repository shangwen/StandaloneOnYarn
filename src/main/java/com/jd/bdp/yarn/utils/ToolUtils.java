package com.jd.bdp.yarn.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.Map;

/**
 * Created by tangshangwen on 17-3-16.
 */
public class ToolUtils {
    public static LocalResource createResource(Configuration conf, String src) throws IOException {
        LocalResource resource = Records.newRecord(LocalResource.class);
        Path tarPath = new Path(src);
        FileSystem fs = tarPath.getFileSystem(conf);
        FileStatus fileStatus = fs.getFileStatus(tarPath);
        resource.setResource(ConverterUtils.getYarnUrlFromPath(tarPath));
        resource.setSize(fileStatus.getLen());
        resource.setTimestamp(fileStatus.getModificationTime());
        resource.setType(LocalResourceType.FILE);
        resource.setVisibility(LocalResourceVisibility.PUBLIC);
        return resource;
    }

    /**
     * 设定优先级
     * @param p
     * @return
     */
    public static Priority getPriority(int p) {
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(p);
        return pri;
    }

    public static Configuration loadProp2Conf(Map<String, String> config,
                                              Configuration conf) {
        for(Map.Entry<String, String> entry : config.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }
}
