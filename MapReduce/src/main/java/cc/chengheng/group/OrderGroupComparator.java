package cc.chengheng.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组，默认是k2全相同才会分一个组，现在需要前部分数据相同才就分组，不看价格
 */

public class OrderGroupComparator extends WritableComparator {

    public OrderGroupComparator() {
        super(OrderBean.class, true);
    }

    /**
     * 指定分组的规则
     *
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 对形参做强制类型转换
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;

        // 指定分组规则
        return first.getOrderId().compareTo(second.getOrderId());
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
