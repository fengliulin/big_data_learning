package cc.chengheng.casus.flow3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    /**
     * k1和v1 转为 k2和v2
     * ------------------------
     * k1           v1
     * 0        1363157985066	13726230523	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com	游戏娱乐	24	27	2481	24681	200
     * 转
     * k2               v2
     * 1363157985066    FlowBean(24	27	2481	24681)
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1、拆分行文本数据，得到手机号--->k2
        String[] split = value.toString().split("\t");
        String phoneNum = split[0];

        // 2、创建FlowBean对象，并从行文本数据拆分出流量的四个字段，给FlowBean对象
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[6]));
        flowBean.setDownFlow(Integer.parseInt(split[7]));
        flowBean.setUpCountFlow(Integer.parseInt(split[8]));
        flowBean.setDownCountFlow(Integer.parseInt(split[9]));

        // 3、将k2和v2写入上下文中
        context.write(new Text(phoneNum), flowBean);
    }
}
