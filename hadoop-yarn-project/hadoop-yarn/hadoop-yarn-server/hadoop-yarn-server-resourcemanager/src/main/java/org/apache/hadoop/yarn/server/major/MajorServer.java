package org.apache.hadoop.yarn.server.major;
import com.sun.tools.javac.code.Attribute;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Majorshi on 16/12/9.
 */
public class MajorServer {
    private static MajorServer instance;
    private boolean isOpen;
    private MajorServerThread thread;
    private HashMap<String, MajorNodeInfo> nodeInfo;
    private HashMap<String, MajorAppInfo> appInfo;
    private HashMap<String, Integer> appMapNumer;
    private HashMap<String, Integer> nodeAvaliableMemory;
    private HashMap<String, Integer> applicationIdleNumber;
    private HashMap<String, List<String>> blockChoices;
    private HashMap<String, HashMap<String, Integer>> allocatedContainers;
    private HashMap<String, HashMap<String, ArrayList<String>>> applicationTaskHostMap;
    private HashMap<String, HashMap<String, ArrayList<String>>> applicationHostTaskMap;
    private HashMap<String, HashMap<String, String>> applicationTaskContainerMap;
    private HashMap<String, HashMap<String, MajorContainerPlan>> containerPlan;
    private HashMap<String, Integer> applicationUnlocalTaskNum;
    private HashMap<String, Long> applicationStartTime;
    private static final Log LOG = LogFactory.getLog("Major");
    private static final Log StatLOG = LogFactory.getLog("Stat");
    private MajorServer (){}
    public static synchronized MajorServer getInstance() {
        if (instance == null) {
            instance = new MajorServer();
            instance.isOpen = false;
            instance.thread = null;
            instance.nodeInfo = new HashMap<String, MajorNodeInfo>();
            instance.appInfo = new HashMap<String, MajorAppInfo>();
            instance.blockChoices = new HashMap<String, List<String>>();
            instance.allocatedContainers = new HashMap<String, HashMap<String, Integer>>();
            instance.appMapNumer = new HashMap<String, Integer>();
            instance.containerPlan = new HashMap<String, HashMap<String, MajorContainerPlan>>();
            instance.nodeAvaliableMemory = new HashMap<String, Integer>();
            instance.applicationTaskHostMap = new HashMap<String, HashMap<String, ArrayList<String>>>();
            instance.applicationHostTaskMap = new HashMap<String, HashMap<String, ArrayList<String>>>();
            instance.applicationTaskContainerMap = new HashMap<String, HashMap<String, String>>();
            instance.applicationStartTime = new HashMap<String, Long>();
            instance.applicationIdleNumber = new HashMap<String, Integer>();
            instance.applicationUnlocalTaskNum = new HashMap<String, Integer>();
        }
        return instance;
    }

    public boolean run() {
        if (!instance.isOpen) {
            instance.thread = new MajorServerThread("MajorServer");
            LOG.info("======================================================");
            LOG.info("|                  MAJOR SERVER LOG                  |");
            LOG.info("======================================================");
            LOG.info("| Major Server 正在初始化");
            instance.thread.start();
            instance.isOpen = true;
            LOG.info("| Major Server 启动成功！");
            return instance.isOpen;
        } else {
            return true;
        }
    }

    public boolean getisOpen() {
        return instance.isOpen;
    }

    public HashMap<String, MajorNodeInfo> getNodeInfo() {
        return nodeInfo;
    }

    public HashMap<String, MajorAppInfo> getAppInfo() {
        return appInfo;
    }

    public HashMap<String, List<String>> getBlockChoices() {
        return blockChoices;
    }

    public HashMap<String, HashMap<String, Integer>> getAllocatedContainers() {
        return allocatedContainers;
    }

    public void updateUnlocalTask(String appid) {
        appid = formatAppId(appid);
        if (instance.applicationUnlocalTaskNum.containsKey(appid)) {
            instance.applicationUnlocalTaskNum.put(appid, instance.applicationUnlocalTaskNum.get(appid) + 1);
        }
    }

    public String formatAppId (String id) {
        if (id.startsWith("application")) {
            if (id.contains("_")) {
                String[] sps = id.split("_");
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < sps.length; i++) {
                    if (i == 0) continue;
                    if (sb.length() != 0) {
                        sb.append("_");
                    }
                    sb.append(sps[i]);
                }
                return sb.toString();
            }
            else
                return id;
        } else {
            return id;
        }

    }

    public String formatNodeId (String id) {
        if (id.contains(":"))
            return id.split(":")[0];
        else
            return id;
    }

    public void updateNodeInfo(String nodeid, double bandwidth) {
        String nid = formatNodeId(nodeid);
        if (!nodeInfo.containsKey(nid)) {
            MajorNodeInfo ninfo = new MajorNodeInfo(nid);
            nodeInfo.put(nid, ninfo);
        }
        MajorNodeInfo nodeinfo = nodeInfo.get(nid);
        nodeinfo.bandwithUsed = bandwidth;
    }

    public void logChooseDataNode(String result, String host) {
        LOG.info("| 对 " + host + " 的Block请求结果为 " + result);
    }

    public void addApplication(String id, int num) {
        instance.appMapNumer.put(formatAppId(id), num);
        instance.containerPlan.put(formatAppId(id), new HashMap<String, MajorContainerPlan>());
        LOG.info("||||||||||||||||||||||||||||||||||||||发现新任务 " + id + " Mapper数量为 " + num + "||||||||||||||||||||||||||||||||||||||");
        instance.applicationStartTime.put(formatAppId(id), System.currentTimeMillis());
        instance.applicationIdleNumber.put(formatAppId(id), 0);
        instance.applicationUnlocalTaskNum.put(formatAppId(id), 0);
    }

    public void updateAppInfo(String AMhostname, String AppattemptID, int numtotalContainer, int numCompletedContainers, int numAllocatedContainers, int numFailedContainers, int numRequestedContainer) {
//        MajorAppInfo ap = new MajorAppInfo(AMhostname, AppattemptID, numtotalContainer, numCompletedContainers, numAllocatedContainers, numFailedContainers, numRequestedContainer);
//        instance.appInfo.put(AppattemptID, ap);
        LOG.info("| 收到来自ApplicationMaster " + AMhostname + " 的更新信息 " + AppattemptID);
    }

    public void updateBlockChoices(List<String> nodeids, String blockid) {
        instance.blockChoices.put(blockid + "|" + instance.blockChoices.size(), nodeids);
        StringBuilder sb = new StringBuilder();
        sb.append("| 收到来自 ");
        sb.append(blockid);
        sb.append(" 的Block读取请求, 可选节点为 ");
        for (String id: nodeids) {
            sb.append("|");
            sb.append(id);
            if (instance.getNodeInfo().get(id) != null) {
                DecimalFormat df = new DecimalFormat("######0.0000");
                sb.append("(");
                sb.append(df.format(instance.getNodeInfo().get(id).bandwithUsed));
                sb.append("%)");
            }
        }
        LOG.info(sb.toString());
    }

    public void updateIdleNum(String appid) {
        appid = formatAppId(appid);
        instance.applicationIdleNumber.put(appid, instance.applicationIdleNumber.get(appid) - 1);
    }

    public void updateResourceRequest(List<ResourceRequest> ask, String appid) {

    }

    public int getAllocatedContainerNumber(String nodeid, String appid, int requestCapability, int newMemory, int assignableContainers) {
        int avaliableContainers = newMemory / requestCapability;
        String nid = formatNodeId(nodeid);
        if (instance.getNodeInfo().containsKey(nid)) {
            int majorAvaliable = instance.getNodeInfo().get(nid).memory / requestCapability;
            LOG.info("|| 记录的关于 " + nid + " 的资源数据:");
            LOG.info("|| 记录的Memory:" + instance.getNodeInfo().get(nid).memory + "| " + newMemory);
            LOG.info("|| 记录所得avaliableContainers:" + majorAvaliable + "| " + avaliableContainers);
        }
        return Math.min(avaliableContainers, assignableContainers);
    }

    public void reportAllocatedContainerNumber(String nodeid, String appid, int allocatedContainers) {
        if (instance.containerPlan.get(formatAppId(appid)) != null) {
            if (instance.containerPlan.get(formatAppId(appid)).get(formatNodeId(nodeid)) != null) {
                MajorContainerPlan plan = instance.containerPlan.get(formatAppId(appid)).get(formatNodeId(nodeid));
                plan.assignedContainerNum += allocatedContainers;
                plan.containerNumToAssign -= allocatedContainers;
                LOG.info("| 应用 " + formatAppId(appid) + " 在节点 " + formatNodeId(nodeid) + " 上的分配情况: " + plan.assignedContainerNum + "|" + plan.containerNumToAssign);
            }
        }
    }

    public void doneApplication(String appid) {
        String apid = formatAppId(appid);
        LOG.info("| 应用 " + apid + " 已完成");
        LOG.info("||||||||||||||||||||||||||||||||||||||华丽的应用分割线||||||||||||||||||||||||||||||||||||||");
        if (instance.applicationStartTime.containsKey(apid)) {
            long now = System.currentTimeMillis();
            long start = instance.applicationStartTime.get(apid);
//            int idlenum = instance.applicationIdleNumber.get(apid);
            int unlocalnum = instance.applicationUnlocalTaskNum.get(apid);
            int allnum = instance.appMapNumer.get(apid);
            StatLOG.info((now - start) + "|" + unlocalnum + "|" + allnum);
            instance.applicationStartTime.remove(apid);
        }
        if (instance.applicationUnlocalTaskNum.containsKey(apid)) {
            instance.applicationUnlocalTaskNum.remove(apid);
        }
        if (instance.appMapNumer.containsKey(apid)) {
            instance.appMapNumer.remove(apid);
        }
        if (instance.applicationIdleNumber.containsKey(apid)) {
            instance.applicationIdleNumber.remove(apid);
        }
    }

    public void updateNodeAvaliableMemory(String nodeid, int memory) {
        String nid = formatNodeId(nodeid);
        if (!nodeInfo.containsKey(nid)) {
            MajorNodeInfo ninfo = new MajorNodeInfo(nid);
            nodeInfo.put(nid, ninfo);
        }
        MajorNodeInfo nodeinfo = nodeInfo.get(nid);
        nodeinfo.memory = memory;
        nodeinfo.lastReportTime = System.currentTimeMillis();
//        LOG.info("|||| " + nid + " update.Memory:" + memory + "|" + nodeinfo.lastReportTime);
    }

    public void updateTaskInfo(String appid, Map<String,List<String>> taskinfo, int totalContainers) {
        LOG.info("=====updateTaskInfo: " + appid);
        appid = formatAppId(appid);
        if (!instance.applicationTaskHostMap.containsKey(appid)) {
            instance.applicationTaskHostMap.put(appid, new HashMap<String, ArrayList<String>>());
        }
        if (!instance.applicationHostTaskMap.containsKey(appid)) {
            instance.applicationHostTaskMap.put(appid, new HashMap<String, ArrayList<String>>());
        }
        HashMap<String, ArrayList<String>> taskhostmap = instance.applicationTaskHostMap.get(appid);
        HashMap<String, ArrayList<String>> hosttaskmap = instance.applicationHostTaskMap.get(appid);
        for (String taskid: taskinfo.keySet()) {
            String str = taskid + ": ";
            ArrayList<String> taskhosts = new ArrayList<String>();
            for (String host : taskinfo.get(taskid)) {
                taskhosts.add(formatNodeId(host));
                str += host + "|";
            }
            taskhostmap.put(taskid, taskhosts);
            LOG.info("##### " + str);
        }
        //add taskid to hosttaskmap
        for (String taskid: taskinfo.keySet()) {
            //clear old data
            for (String host : hosttaskmap.keySet()) {
                ArrayList<String> tasks = hosttaskmap.get(formatNodeId(host));
                if (tasks.contains(taskid)) {
                    tasks.remove(taskid);
                }
            }
            // add new data
            for (String host : taskinfo.get(taskid)) {
                if (!hosttaskmap.containsKey(formatNodeId(host))) {
                    hosttaskmap.put(host, new ArrayList<String>());
                }
                hosttaskmap.get(formatNodeId(host)).add(taskid);
            }
        }
    }

    public void allocateContainers(String appid, ArrayList<ContainerId> cids, String host) {
        LOG.info("=====allocateContainers: " + appid + "| cids:" + cids.size() + "|" + host);
        //给container分配task
        appid = formatAppId(appid);
        String nodeid = formatNodeId(host);
        HashMap<String, ArrayList<String>> hosttaskmap = instance.applicationHostTaskMap.get(appid);
        boolean containerIsRest = true;
        if (!instance.applicationTaskContainerMap.containsKey(appid)) {
            instance.applicationTaskContainerMap.put(appid, new HashMap<String, String>());
        }
        HashMap<String, String> taskcontainermap = instance.applicationTaskContainerMap.get(appid);
        if (hosttaskmap.containsKey(nodeid)) {
            ArrayList<String> tasklist = hosttaskmap.get(nodeid);
            if (!instance.applicationTaskContainerMap.containsKey(appid)) {
                instance.applicationTaskContainerMap.put(appid, new HashMap<String, String>());
            }
            ArrayList<String> newaddtask = new ArrayList<String>();
            for (String taskid : tasklist) {
                if (cids.size() > 0) {
                    ContainerId cid = cids.get(0);
                    cids.remove(cid);
                    taskcontainermap.put(taskid, cid.toString());
                    newaddtask.add(taskid);
                } else {
                    break;
                }
            }
            if (cids.size() == 0) {
                containerIsRest = false;
            }
            //删掉该task需求container的记录
            for (String taskid : newaddtask) {
                for (String host2 : hosttaskmap.keySet()) {
                    ArrayList<String> tasks = hosttaskmap.get(formatNodeId(host2));
                    if (tasks.contains(taskid)) {
                        tasks.remove(taskid);
                    }
                }
            }
        }
        if (containerIsRest) {
            //container多了，要分给别的host的task
            for (String otherhost : hosttaskmap.keySet()) {
                if (cids.size() == 0) break;
                ArrayList<String> othertasks = hosttaskmap.get(formatNodeId(otherhost));
                ArrayList<String> newaddtask = new ArrayList<String>();
                for (String taskid : othertasks) {
                    if (cids.size() > 0) {
                        ContainerId cid = cids.get(0);
                        cids.remove(cid);
                        taskcontainermap.put(taskid, cid.toString());
                        newaddtask.add(taskid);
                    } else {
                        break;
                    }
                }
                //删掉该task需求container的记录
                for (String taskid : newaddtask) {
                    for (String host2 : hosttaskmap.keySet()) {
                        ArrayList<String> tasks = hosttaskmap.get(formatNodeId(host2));
                        if (tasks.contains(taskid)) {
                            tasks.remove(taskid);
                        }
                    }
                }
            }
        }
        //log
        LOG.info("**** 对于应用 " + appid + " 在 " + host + " 上的分配结果为:");
        for (String taskid : instance.applicationTaskContainerMap.get(appid).keySet()) {
            String cid = instance.applicationTaskContainerMap.get(appid).get(taskid);
            LOG.info("**** Task: " + taskid + " | Container: " + cid);
        }
    }

    public HashMap<String, String> getTaskContainerMap(String appid, List<String> containerids) {
        HashMap<String, String> newmap = new HashMap<String, String>();
        if (instance.applicationTaskContainerMap.containsKey(formatAppId(appid))) {
            HashMap<String, String> taskcontainermap = instance.applicationTaskContainerMap.get(formatAppId(appid));
            for (String taskid : taskcontainermap.keySet()) {
                String cid = taskcontainermap.get(taskid);
                if (containerids.contains(cid)) {
                    newmap.put(cid, taskid);
                }
            }
        }
        return newmap;
    }
}
