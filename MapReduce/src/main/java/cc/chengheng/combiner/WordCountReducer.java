package cc.chengheng.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 *      KEYIN: k2类型
 *      VALUEIN:v2 类型
 *      KEYOUT: k3 类型
 *      VALUEOUT: v3 类型
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * 将新的k2和v2转为k3和v3，将k3和v3写入上下文中 <br>
     * 如何将新的k2和v2转为k3和v3
     * k2           v2
     * hello        <1,1,1>
     * world        <1,1>
     * hadoop       <1>
     * ---------------------------------
     * k3       v3
     * hello    3
     * world    2
     * hadoop   1
     *
     * @param key 新k2
     * @param values 集合 新v2
     * @param context 上下文对象
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
