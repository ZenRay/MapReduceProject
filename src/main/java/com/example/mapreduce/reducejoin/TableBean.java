package com.example.mapreduce.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {
    private String orderId;
    private String productId;
    private Integer amount;
    private String productName;
    private String tableFlag;


    public TableBean() {
    }

    public String getTableFlag() {
        return tableFlag;
    }

    public void setTableFlag(String tableFlag) {
        this.tableFlag = tableFlag;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    @Override
    public String toString() {
        // 输出时仅限制了需要的信息
        return orderId + "\t" + productName + "\t" + amount + "\t" + tableFlag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(productId);
        out.writeInt(amount);
        out.writeUTF(productName);
        out.writeUTF(tableFlag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.productId = in.readUTF();
        this.amount = in.readInt();
        this.productName = in.readUTF();
        this.tableFlag = in.readUTF();
    }
}
