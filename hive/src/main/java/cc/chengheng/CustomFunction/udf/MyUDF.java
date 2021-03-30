package cc.chengheng.CustomFunction.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * hive自定义函数, 项目打jar包，上传到hive的lib目录
 */
public class MyUDF extends UDF {

    /**
     * 模拟Hive的upper方法：将字符串的第一个字符转为大写，而其他字符不变
     *
     * @param line
     * @return
     */
    public Text evaluate(final Text line) {
        if (line.toString() != null && !line.toString().equals("")) {
            String str = line.toString().substring(0, 1).toUpperCase() + line.toString().substring(1);

            return new Text(str);
        }
        return new Text("");
    }
}
