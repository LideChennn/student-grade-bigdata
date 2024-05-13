package cn.edu.gdpu.lastwork.two;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GradePartitioner extends Partitioner<GradeBean, NullWritable> {

    @Override
    public int getPartition(GradeBean gradeBean, NullWritable nullWritable, int i) {
        int part;
        String province = gradeBean.getProvince();
        switch (province) {
            case "广东":
                part = 0;
                break;
            case "江苏":
                part = 1;
                break;
            case "浙江":
                part = 2;
                break;
            default:
                part = -1;
                break;
        }
        return part;
    }
}
