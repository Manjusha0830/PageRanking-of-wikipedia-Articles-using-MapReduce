import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;
import java.util.List;
import java.io.IOException;

public class InitRankMapper extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String pageAndLinksString = value.toString();
		List<String> pageAndLinksList = Arrays.asList(pageAndLinksString.split("\\t"));
		String links ="";
		String page  = pageAndLinksList.get(0);//Assuming there are no blank rows, we can check for 0 size List
		if(pageAndLinksList.size()>0)//if there are links add them to links
			links = String.join("\t",pageAndLinksList.subList(1,pageAndLinksList.size()));
        // Map Links to the page for reduce input
        context.write(new Text(page), new Text( links));
    }
}
