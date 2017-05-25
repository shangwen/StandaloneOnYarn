package com.jd.bdp.yarn.client;

import com.jd.bdp.yarn.thrift.WorkerInfo;
import com.jd.bdp.yarn.utils.ContextFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by tangshangwen on 17-3-15.
 */
public class RMClient implements AMRMClientAsync.CallbackHandler {

    private static final Log LOG = LogFactory.getLog(RMClient.class);

    private AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync;
    private NMClient nmClient;
    private ConcurrentHashMap<String, Container> currentWorkers;
    private List<String> allocatedNodeHost;
    private List<String> allocatedNodeIp;
    private Configuration conf;
    private WorkerInfo workerInfo;

    public RMClient(Configuration conf, WorkerInfo workerInfo) {
        this.amrmClientAsync = AMRMClientAsync.createAMRMClientAsync(100, this);
        this.nmClient = NMClient.createNMClient();
        this.conf = conf;
        this.workerInfo = workerInfo;

        this.amrmClientAsync.init(conf);
        this.amrmClientAsync.start();

        this.nmClient.init(conf);
        this.nmClient.start();

        this.currentWorkers = new ConcurrentHashMap<>();
        this.allocatedNodeHost = new CopyOnWriteArrayList<>();
        this.allocatedNodeIp = new CopyOnWriteArrayList<>();

        registerApplicationMaster();
    }

    /**
     * 创建container请求
     *
     * @param resource
     * @param nodes
     * @param racks
     * @param priority
     * @param relaxLocality
     * @return
     */
    public AMRMClient.ContainerRequest buildContainer(
            Resource resource, String[] nodes, String[] racks, Priority priority, boolean relaxLocality) {
        AMRMClient.ContainerRequest workContainer = new AMRMClient.ContainerRequest(
                resource, nodes, racks, priority, relaxLocality);
        return workContainer;
    }

    public void addContainerRequest(AMRMClient.ContainerRequest containerRequest) {
        amrmClientAsync.addContainerRequest(containerRequest);
    }

    private void registerApplicationMaster() {
        try {
            amrmClientAsync.registerApplicationMaster("", 0, "");
        } catch (Exception e) {
            LOG.error("registerApplicationMaster Failed! exiting", e);
            System.exit(1);
        }
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> list) {
        for (ContainerStatus status : list) {

            LOG.info("Completed containerId:" + status.getContainerId() + " ExitStatus:" + status.getExitStatus());

            if (currentWorkers.containsKey(status.getContainerId())) {
                String host = currentWorkers.get(status.getContainerId()).getNodeId().getHost();
                LOG.info("Removed container:" + status.getContainerId() + " in Host:" + host);
                this.allocatedNodeHost.remove(host);
                currentWorkers.remove(status.getContainerId());
                LOG.info("Removed container finish!");
            }
        }
    }

    /**
     * 判断是否释放container
     *
     * @param host
     * @return
     */
    private boolean isRelease(String host) {
        LOG.info("allocatedNodeHost.contains(host)=" + allocatedNodeHost.contains(host)
                + " host:" + host);
        LOG.info("allocatedNodeHost.size() >= workerInfo.getNumWorkers()=" +
                (this.allocatedNodeHost.size() >= workerInfo.getNumWorkers()));
        LOG.info("allocatedNodeHost size=" + allocatedNodeHost.size());
        LOG.info("workerInfo getNumWorkers=" + workerInfo.getNumWorkers());
        if (this.allocatedNodeHost.contains(host) ||
                this.allocatedNodeHost.size() >= workerInfo.getNumWorkers()) {
            return true;
        }
        return false;
    }

    @Override
    public void onContainersAllocated(List<Container> list) {
        for (Container container : list) {
            if (isRelease(container.getNodeId().getHost())) {
                LOG.info("isRelease=true");
                amrmClientAsync.releaseAssignedContainer(container.getId());
                continue;
            }
            LOG.info("build Container LaunchContext...");
            try {
                ContainerLaunchContext ctx = ContextFactory.createLanchContext(conf);
                nmClient.cleanupRunningContainersOnStop(false);
                nmClient.startContainer(container, ctx);
                currentWorkers.put(container.getId().toString(), container);
                allocatedNodeHost.add(container.getNodeId().getHost());
            } catch (Exception e) {
                LOG.error("Exception ", e);
            }
        }
    }

    @Override
    public void onShutdownRequest() {

    }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {
        for (NodeReport node : list) {
            LOG.info("onNodesUpdated NodeId=" + node.getNodeId()
                    + " HttpAddress=" + node.getHttpAddress()
                    + " NumContainers" + node.getNumContainers());
        }
    }

    public int getCurrentWorksSize() {
        return currentWorkers.size();
    }

    public synchronized void stopWorkers(int num) {
        for (String containerId : currentWorkers.keySet()) {
            if (num <= 0) {
                break;
            }
            Container container = currentWorkers.get(containerId);
            amrmClientAsync.releaseAssignedContainer(container.getId());
            LOG.info("stopWorker container:" + containerId + " on Host:" + container.getNodeHttpAddress());
            currentWorkers.remove(containerId);
            num--;
        }
        printWorkers();
    }

    public synchronized void stopAllWorkers() {
        LOG.info("Stop All Workers...");
        stopWorkers(currentWorkers.size());
    }

    public void printWorkers() {
        LOG.info("----------------Workers----------------");
        for (String containerId : currentWorkers.keySet()) {
            Container container = currentWorkers.get(containerId);
            LOG.info("Worker container:" + containerId + " on Host:" + container.getNodeHttpAddress());
        }
        LOG.info("---------------------------------------");
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable throwable) {
        try {
            if (amrmClientAsync != null) {
                amrmClientAsync.close();
            }
            if (nmClient != null) {
                nmClient.close();
            }
        } catch (IOException e) {
            LOG.error("close client error", e);
        }
    }
}
