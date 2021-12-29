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

public class DocumentCount {

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
            System.out.println("reduce:" + key + ',' + result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "document count");
        job.setJarByClass(DocumentCount.class);
//        job.setInputFormatClass(FileNameInputFormat.class);
        job.setInputFormatClass(FileNameCombineInputFormat.class);
//        job.setMapperClass(DocumentCount.TokenizerMapper.class);
//        job.setCombinerClass(DocumentCount.IntSumReducer.class);
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
        FileInputFormat.setMaxInputSplitSize(job, 3145728);
        FileInputFormat.addInputPaths(job, paths.toString().substring(1));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}