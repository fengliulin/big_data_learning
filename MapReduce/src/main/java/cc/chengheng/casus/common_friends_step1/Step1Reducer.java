package cc.chengheng.casus.common_friends_step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Step1Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 1、遍历集合，并将每一个元素拼接，得到k3
        StringBuffer buffer = new StringBuffer();
        for (Text value : values) {
            buffer.append(value.toString()).append("-");
        }

        // 2、k2就是v3
        // 3、将k3和v3写入上下文中
        context.write(new Text(buffer.toString()), key);
    }
}
