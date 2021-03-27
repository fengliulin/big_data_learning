package cc.chengheng.casus.flow3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowCountPartition extends Partitioner<Text, FlowBean> {

    /**
     * 指定分区的规则
     *
     * 参数
     * text:k2 手机号
     * flowBean: v2
     * i: ReduceTask的个数
     *
     * @param text
     * @param flowBean
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {

        // 获取手机号
        String phoneNum = text.toString();
        if (phoneNum.startsWith("135")) {
            return 0; // 分区编号文件名  part-r-00000
        } else if (phoneNum.startsWith("136")) {
            return 1; // 分区编号文件名  part-r-00001
        } else if (phoneNum.startsWith("137")) {
            return 2; // 分区编号文件名  part-r-00002
        } else {
            return 3; // 分区编号文件名  part-r-00003
        }
    }
}
