package cc.chengheng.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, NullWritable> {

    /**
     * 定义分区规则
     *
     * @param text          k2
     * @param nullWritable  v2
     * @param numPartitions
     * @return 返回对应的分区编号
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int numPartitions) {
        // 1、拆分行文本数据 k2 ， 获取中奖字段的值
        String numStr = text.toString().split(",")[6];

        // 2、判断中奖字段的值和15的关系，然后返回对应的分区编号
        if (Integer.parseInt(numStr) > 15) {
            return 1;
        } else {
            return 0;
        }
    }
}
