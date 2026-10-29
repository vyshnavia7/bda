package word;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MostFrequentWords {
	 // Mapper: swap <word, count> → <count, word>
	  public static class SwapMapper extends Mapper<Object, Text, IntWritable, Text> {
	    private IntWritable count = new IntWritable();
	    private Text word = new Text();
	    public void map(Object key, Text value, Context context)
	    	      throws IOException, InterruptedException {
	    	      String[] parts = value.toString().trim().split("\\s+");
	    	      if (parts.length == 2) {
	    	        try {
	    	          word.set(parts[0]);
	    	          count.set(Integer.parseInt(parts[1]));
	    	          context.write(count, word);
	    	        } catch (NumberFormatException e) {
	    	        	//malformed lines
	    	        }
	    	      }}}
	// Reducer: just output in descending order
	  public static class DescComparator extends IntWritable.Comparator {
		    @Override
		    public int compare(byte[] b1, int s1, int l1,
		                       byte[] b2, int s2, int l2) {
		        return -super.compare(b1, s1, l1, b2, s2, l2);
		    }
		}
	  public static class DescReducer
      extends Reducer<IntWritable, Text, IntWritable, Text> {
  public void reduce(IntWritable key, Iterable<Text> values,
                     Context context)
          throws IOException, InterruptedException {
      for (Text val : values) {
          context.write(key, val);
      }
  }
}
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "most frequent words");

		    job.setJarByClass(MostFrequentWords.class);
		    job.setMapperClass(SwapMapper.class);
		    job.setReducerClass(DescReducer.class);
		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);

		    job.setSortComparatorClass(DescComparator.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}}
