


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;




public class PageRanker extends Configured implements Tool {

    
    static String inputPath;
    static String outputPath;

    public static void main(String[] args) throws Exception {

        inputPath =  args[0];
		
        outputPath = args[1];


        System.exit(ToolRunner.run(new Configuration(), new PageRanker(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        boolean isCompleted = initializePageRanks(inputPath, outputPath + "/1");
        if (!isCompleted) return 1;

        String lastResultPath = null;

        for (int currentRun = 1; currentRun < 10; currentRun++) {
            String inPath = outputPath +"/" + currentRun;
            lastResultPath = outputPath +"/" + (currentRun+ 1);

            isCompleted = calculateRun(inPath, lastResultPath);

            if (!isCompleted) return 1;
        }

      
        return 0;
    }


    public boolean initializePageRanks(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankIntializer = Job.getInstance(conf, "runInitPageRanking");
        rankIntializer.setJarByClass(PageRanker.class);

        rankIntializer.setOutputKeyClass(Text.class);
        rankIntializer.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankIntializer, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankIntializer, new Path(outputPath));

        rankIntializer.setMapperClass(InitRankMapper.class);
        rankIntializer.setReducerClass(InitRankReduce.class);

        return rankIntializer.waitForCompletion(true);
    }

    private boolean calculateRun(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankCalculator = Job.getInstance(conf, "rankCalculator");
        rankCalculator.setJarByClass(PageRanker.class);

        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

        rankCalculator.setMapperClass(RankCalculateMapper.class);
        rankCalculator.setReducerClass(RankCalculateReduce.class);

        return rankCalculator.waitForCompletion(true);
    }


}
