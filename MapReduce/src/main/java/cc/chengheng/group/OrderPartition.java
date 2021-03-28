package cc.chengheng.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartition extends Partitioner<OrderBean, Text> {

    /**
     * 分区规则：根据订单的id实现分区
     *
     * @param orderBean k2
     * @param text v2
     * @param numPartitions ReduceTask个数，就是输出的文件分开
     * @return 返回分区的编号
     */
    @Override
    public int getPartition(OrderBean orderBean, Text text, int numPartitions) {
        System.out.println();

        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
