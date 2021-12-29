import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ClassProbPair implements WritableComparable<ClassProbPair>{
    private String className;
    private double probLog;

    public ClassProbPair() {
        // TODO Auto-generated constructor stub
    }

    public ClassProbPair(String className, double probLog)
    {
        this.className = className;
        this.probLog = probLog;
    }

    @Override
    public void write(DataOutput out) throws IOException {  //序列化
        out.writeBytes(className + "\n");
        out.writeDouble(probLog);
    }
    @Override
    public void readFields(DataInput in) throws IOException {   //反序列化
        this.className = in.readLine();
        this.probLog = in.readDouble();
    }
    @Override
    public int compareTo(ClassProbPair o) { //自定义比较函数
        int c1 = this.className.compareTo(o.className);
        if (c1 == 0) {
            return Double.compare(this.probLog, o.probLog);
        } else {
            return c1;
        }
    }

    @Override
    public String toString() {
        return this.className + "\t" + this.probLog;
    }

    public String getClassName() {
        return className;
    }

    public double getProbLog() {
        return probLog;
    }

    public void set(ClassProbPair o) {
        this.setClassName(o.className);
        this.setProbLog(o.probLog);
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public void setProbLog(double probLog) {
        this.probLog = probLog;
    }
}
