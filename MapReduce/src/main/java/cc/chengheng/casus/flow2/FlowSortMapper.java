package cc.chengheng.casus.flow2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    /**
     * k1:LongWritable 行偏移量
     * v1:Text 行文本数据
     *
     * k2：FlowBean
     * V2：Text 手机号
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1、拆分行文本数据v1，得到四个流量字段，并封装FlowBean对象---- k2
        String[] split = value.toString().split("\t");

        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[6]));
        flowBean.setDownFlow(Integer.parseInt(split[7]));
        flowBean.setUpCountFlow(Integer.parseInt(split[8]));
        flowBean.setDownCountFlow(Integer.parseInt(split[9]));

        // 2、通过行文本数据，得到手机号----v2
        String phoneNum = split[0];

        // 3、将k2和v2写入上下文中
        context.write(flowBean, new Text(phoneNum));
    }
}
