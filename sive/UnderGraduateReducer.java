package cn.edu.gdpu.lastwork.sive;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UnderGraduateReducer extends Reducer<GradeBean, NullWritable, GradeBean, NullWritable> {
    @Override
    protected void reduce(GradeBean key, Iterable<NullWritable> values, Reducer<GradeBean, NullWritable, GradeBean, NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
