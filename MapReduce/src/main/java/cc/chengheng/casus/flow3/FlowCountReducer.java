package cc.chengheng.casus.flow3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 1、遍历集合，并将集合中的对应的四个字段累计
        Integer upFlow = 0; // 上行数据包
        Integer downFlow = 0; // 下行数据包数
        Integer upCountFlow = 0; //上行流量总和
        Integer downCountFlow = 0; // 下行流量总和

        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }

        // 2、创建FlowBean对象，并给对象赋值
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);

        // 3、将k3和v3写入上下文中
        context.write(key, flowBean);
    }
}
