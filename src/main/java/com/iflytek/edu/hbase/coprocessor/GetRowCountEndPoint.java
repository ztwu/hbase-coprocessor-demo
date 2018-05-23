package com.iflytek.edu.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2018/3/23
 * Time: 16:46
 * Description
 */

public class GetRowCountEndPoint extends GetRowCount.iflytekEduService implements Coprocessor,CoprocessorService {

    private RegionCoprocessorEnvironment env;  // 定义环境

    // 协处理器初始化时调用的方法
    public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        if (coprocessorEnvironment instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)coprocessorEnvironment;
        } else {
            throw new CoprocessorException("no load region");
        }
    }

    public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        // no do
    }

    public Service getService() {
        return this;
    }

    public void getRowCount(RpcController controller, GetRowCount.getRowCountRequest request, RpcCallback<GetRowCount.getRowCountResponse> done) {
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        GetRowCount.getRowCountResponse response = null;
        InternalScanner scanner = null;

        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore = false;
            byte[] lastRow = null;
            long count = 0;
            do {
                hasMore = scanner.next(results);
                for (Cell kv : results) {
                    byte[] currentRow = CellUtil.cloneRow(kv);
                    if (lastRow == null || !Bytes.equals(lastRow, currentRow)) {
                        lastRow = currentRow;
                        count++;
                    }
                }
                results.clear();
            } while (hasMore);

            response = GetRowCount.getRowCountResponse.newBuilder().setRowCount(count).build();
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {}
            }
        }
        done.run(response);

    }

}
