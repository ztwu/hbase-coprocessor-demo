package com.iflytek.edu.hbase.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2018/3/26
 * Time: 9:02
 * Description
 */

public class GetRowCountDemo {
    public static void main(String[] args) throws Throwable {
        // TODO Auto-generated method stub
        System.out.println("begin.....");
        long begin_time=System.currentTimeMillis();
        Configuration config= HBaseConfiguration.create();
        byte[] tableName = Bytes.toBytes("ztwu2");

//        config.set("hbase.zookeeper.quorum", "192.168.1.100,192.168.1.101");
//        HBaseAdmin admin = new HBaseAdmin(config);
//        admin.disableTable(tableName);
//        HTableDescriptor htd = admin.getTableDescriptor(tableName);
//        htd.addCoprocessor("com.iflytek.edu.hbase.coprocessor.GetRowCountEndPoint", new Path(
//                        "hdfs://ubuntu1:9000/jars/hbase/hbase-coprocessor-test.jar"), 1001,
//                null);
//        admin.modifyTable(tableName, htd);
//        admin.enableTable(tableName);
//        admin.close();

        String master_ip="192.168.1.101";
        String zk_ip="192.168.1.100";
        String table_name="ztwu2";
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", zk_ip);
        config.set("hbase.master", master_ip+":600000");
        final GetRowCount.getRowCountRequest request = GetRowCount.getRowCountRequest.getDefaultInstance();
        HTable table=new HTable(config,table_name);

        Map<byte[],Long> results = table.coprocessorService(GetRowCount.iflytekEduService.class,
                null, null,
                new Batch.Call<GetRowCount.iflytekEduService,Long>() {
                    public Long call(GetRowCount.iflytekEduService counter) throws IOException {
                        ServerRpcController controller = new ServerRpcController();
                        BlockingRpcCallback<GetRowCount.getRowCountResponse> rpcCallback =
                                new BlockingRpcCallback<GetRowCount.getRowCountResponse>();
                        counter.getRowCount(controller, request, rpcCallback);
                        GetRowCount.getRowCountResponse response = rpcCallback.get();
                        if (controller.failedOnException()) {
                            throw controller.getFailedOn();
                        }
                        return (response != null && response.hasRowCount()) ? response.getRowCount() : 0;
                    }
                });
        table.close();

        if(results.size()>0){
            String hui = results.values().toString();
            String qiang = hui.substring(1, hui.length()-1);
            //results.values()输出为[i]类型，为了方便我这里转化成了i的形式
            System.out.println(qiang);
        }else{
            System.out.println("没有任何返回结果");
        }
        long end_time=System.currentTimeMillis();
        System.out.println("end:"+(end_time-begin_time));
    }
}
