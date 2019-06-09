import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.List;
import java.util.Arrays;
import java.io.IOException;

public class RankCalculateMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String pagewithRankAndLinksString = value.toString();
        List<String> pageWithRankAndLinksList = Arrays.asList(pagewithRankAndLinksString.split("\\t"));

        String page = pageWithRankAndLinksList.get(0);//Assuming there are no blank rows, we can check for 0 size List
        String pageWithRank = String.join("\t",pageWithRankAndLinksList.subList(0,2));//Given input is internal output we can safely assume all pages will have a rankTabIndex
		
        
        //Denote $ to mark that page exists and is not a red link
        context.write(new Text(page), new Text("$"));

        // Skip pages with no links.
        if(pageWithRankAndLinksList.size() <3) 
			return;
		
        List<String> linksList = pageWithRankAndLinksList.subList(2,pageWithRankAndLinksList.size());
        String links = String.join("\t",linksList);
       
        int totalLinks = linksList.size();
        
        for (String link : linksList){
            Text pageRankTotalLinks = new Text(pageWithRank + "\t" +totalLinks);
            context.write(new Text(link), pageRankTotalLinks);
        }
        
        // // Map Links to the page for reduce input and denote with #
        context.write(new Text(page), new Text("#" + links));
    }
}
