import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class DocumentCount {

    // 输入<className,{1,1,...}>，输出<className, docNumber>
    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
//            System.out.println("reduce:" + key + ',' + result);
        }
    }

    /*
    args[0]:训练集目录路径
    args[1]:结果存放路径
    * */
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "document count");
        job.setJarByClass(DocumentCount.class);
//        job.setInputFormatClass(FileNameInputFormat.class);
        job.setInputFormatClass(FileNameCombineInputFormat.class);  // 使用CombineInputFormat处理大量小文件
        // mapper对输入不做处理直接输出
        job.setReducerClass(DocumentCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set trainset
        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path(args[0]);
        FileStatus[] stats = hdfs.listStatus(path);
        StringBuilder paths = new StringBuilder();
        for (FileStatus stat: stats) {
            paths.append(',').append(stat.getPath());
        }

//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileInputFormat.addInputPaths(job, paths.toString().substring(1));
        FileInputFormat.setMaxInputSplitSize(job, 262144);  // 小文件合并成大小最大为256KB的InputSplit
        FileInputFormat.addInputPaths(job, paths.toString().substring(1));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}