package cn.edu.gdpu.lastwork.three;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 输入：
 * 某一科目分区
 * 科目  分数
 *
 * 输出：
 * 科目 分数 （但是区内进来的顺序 >= rank(Top几) 将舍弃 ）
 */
public class GradeTop100Reducer extends Reducer<GradeBean, NullWritable, GradeBean, NullWritable> {

    private int count = 0;
    private int rank;

    @Override
    protected void setup(Reducer<GradeBean, NullWritable, GradeBean, NullWritable>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        rank = Integer.parseInt(conf.get("rank"));
    }

    @Override
    protected void reduce(GradeBean key, Iterable<NullWritable> values, Reducer<GradeBean, NullWritable, GradeBean, NullWritable>.Context context) throws IOException, InterruptedException {
        //接收过来都是排好序了的
        if (count < rank) {
            context.write(key, NullWritable.get());
            count++;
        }
    }
}
