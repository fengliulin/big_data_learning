package cc.chengheng.casus.flow2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * k2 FlowBean
 * v2 Text 手机号
 *
 * k3 Text 手机号
 * v3 FlowBean
 */
public class FlowSortReducer extends Reducer<FlowBean,Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 1、遍历集合，取出k3，并将k3和v3写入上下文中
        for (Text value : values) {
            context.write(value, key);

        }
    }
}
