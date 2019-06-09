import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RankCalculateReduce extends Reducer<Text, Text, Text, Text> {

    private static final double d = 0.85F;
	private static final double N = 1000.0;//Need to find better way to get initial N

    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
       
        
        double probOtherLinksReferred = 0;
        String links = "";
    
		 boolean isNotARedLink = false;
		//String debugText= "\t############";//REMOVE THIS BEFORE SUBMITTING CODE
        
        
        // - check type of value`
        // - calculate prob that this page is hit among all links on other page
        // - add the share to probOtherLinksReferred
        for (Text value : values){
            String valueString = value.toString();
            
            if(valueString.equals("$")) {
                isNotARedLink = true;
                continue;
            }
            
            if(valueString.startsWith("#")){
                links = "\t"+valueString.substring(1);
                continue;
            }
			String[] pageAndRank = valueString.split("\\t");
            
            double pageRank = Double.valueOf(pageAndRank[1]);
            double linksOnRefferedPage = Double.valueOf(pageAndRank[2]);
           
            probOtherLinksReferred += (pageRank/linksOnRefferedPage);
        }

        if(!isNotARedLink) return; // if its red Link there is no rank or rank is 0
        double newPageRank = d * probOtherLinksReferred + ((1-d)/N);
		//write reduce output for page with rank and the reffered links
        context.write(page, new Text(newPageRank + links));
    }
}
