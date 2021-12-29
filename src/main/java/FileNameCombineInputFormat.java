import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class FileNameCombineInputFormat extends CombineFileInputFormat<Text, IntWritable> {
    // exists merely to fix the key/value types and
    // inject the delegate format to the superclass
    // if MyFormat does not use state, consider a constant instead
    private static class CombineMyKeyMyValueReaderWrapper
            extends CombineFileRecordReaderWrapper<Text, IntWritable> {
        protected CombineMyKeyMyValueReaderWrapper(
                CombineFileSplit split, TaskAttemptContext ctx, Integer idx
        ) throws IOException, InterruptedException {
            super(new FileNameInputFormat(), split, ctx, idx);
        }
    }

    @Override
    public RecordReader<Text, IntWritable> createRecordReader(
            InputSplit split, TaskAttemptContext ctx
    ) throws IOException {
        return new CombineFileRecordReader<Text, IntWritable>(
                (CombineFileSplit )split, ctx, CombineMyKeyMyValueReaderWrapper.class
        );
    }
}