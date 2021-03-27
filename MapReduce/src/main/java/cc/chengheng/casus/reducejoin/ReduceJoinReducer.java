package cc.chengheng.casus.reducejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 1、遍历集合，获取v3
        String first = "";
        String second = "";
        for (Text value : values) {
            if (value.toString().startsWith("p")) {
                first = value.toString();
            } else {
                second += value.toString() + "\t";
            }
        }

        // 2、将k3和v3写入上下文中
        context.write(key, new Text(first + "\t" + second));
    }
}
