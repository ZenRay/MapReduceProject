package com.example.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class JoinReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> orders = new ArrayList<>();
        TableBean product = new TableBean();

        for (TableBean value : values) {
            // 为了避免 iterable 中 value 的值是一个对象地址导致数据存储不正确，使用一个临时对象去处理
            TableBean temp = new TableBean();
            try {
                BeanUtils.copyProperties(temp, value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            }

             if("order".equals(value.getTableFlag())){
                orders.add(temp);
            } else if ("product".equals(value.getTableFlag())) {
                try {
                    BeanUtils.copyProperties(product, value);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                }

            }
        }

        // 需要遍历订单数组
        for (TableBean order : orders) {
            order.setProductName(product.getProductName());

            context.write(order, NullWritable.get());
        }

    }
}
