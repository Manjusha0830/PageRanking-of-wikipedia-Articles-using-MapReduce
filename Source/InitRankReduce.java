import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.List;
import java.io.IOException;

public class InitRankReduce extends Reducer<Text, Text, Text, Text> {


    private static final double N = 1000.0;//Need to find better way to get initial N
    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
       
		String links ="\t";
		for (Text value : values){
			 links += value.toString();
		}
        double initRank = 1.0/N;// initialize rank to 1/N as all pages have same probability initially

        context.write(page, new Text(initRank + links));
    }
}
