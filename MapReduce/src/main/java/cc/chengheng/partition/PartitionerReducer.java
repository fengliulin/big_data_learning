package cc.chengheng.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * K2: Text
 * V2: NullWritable
 *
 * k3: Text
 * V3: NullWritable
 */
public class PartitionerReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    public static enum Counter {
        MY_INPUT_RECORDS,
        MY_INPUT_BYTES
    }

    /**
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        // 方式2：使用枚举来定义计数器
        context.getCounter(Counter.MY_INPUT_RECORDS).increment(1L);

        // 第一个参数：k3；第二个参数：v3
        context.write(key, NullWritable.get());
    }
}
