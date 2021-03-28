package cc.chengheng.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;
    private Double price;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" +price;
    }

    /**
     * 指定排序规则
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {
        // 先比较订单id，如果订单id一致，则排序订单金额（降序）
        int i = orderId.compareTo(o.orderId);
        if (i == 0) {
            i = price.compareTo(o.price) * -1; // 降序
        }
        return i;
    }

    /**
     * 对象序列化
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    /**
     * 对象反序列化
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readUTF();
        price = in.readDouble();
    }
}
