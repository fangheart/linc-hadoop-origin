package org.apache.hadoop.Major;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by Majorshi on 16/12/9.
 */
public class MajorClient {
    public static boolean addApplication(int mapnum, String appid){
        try {
            // 设置调用的服务地址为本地，端口为 7911
            TTransport transport = new TSocket("linc-1", 7911);
            transport.open();
            // 设置传输协议为 TBinaryProtocol
            TProtocol protocol = new TBinaryProtocol(transport);
            Major.Client client = new Major.Client(protocol);
            boolean re = client.addApplication(mapnum, appid);
            transport.close();
            return re;
        } catch (TTransportException e) {
            e.printStackTrace();
            return false;
        } catch (TException e) {
            e.printStackTrace();
            return false;
        }
    }
}
