package cc.chengheng.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     *
     * @param key k2
     * @param values v2 <1,1,1,1>
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        // 1、遍历集合，将集合种的数字相加，得到v3
        for (LongWritable value : values) {
            count += value.get();
        }

        // 2、将k2和v3写入上下文
        context.write(key, new LongWritable(count));
    }
}
