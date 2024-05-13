package cn.edu.gdpu.lastwork.four;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumAndCountBean implements Writable {
    private int sum;
    private int count;
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(sum);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sum = dataInput.readInt();
        this.count = dataInput.readInt();
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
