package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;

import org.apache.spark.api.java.function.MapGroupsFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWithQuery;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.TotalTF;

/**
 * This mapGroups function that maps individual term frequency in current document from ArticleWithQuery to the 
 * the total term frequency in Corpus of a query's terms.
 *
 */


public class TotalTFInCorpus implements MapGroupsFunction<Query, ArticleWithQuery, Tuple2<Query,TotalTF>>{
	
	private static final long serialVersionUID = -6456363146611418557L;
	
	public Tuple2<Query,TotalTF> call(Query key, Iterator<ArticleWithQuery> values) throws Exception {
		
		int[] totalTermFrequencyInCorpus = new int[key.getQueryTermCounts().length];
		TotalTF tf = new TotalTF();
		
		while (values.hasNext()) {
			ArticleWithQuery article = values.next();
			
			for(int i =0; i<article.getTermFrequencyInCurrentDocument().length; i++) {
				
				//all values are added index-wise to form a resultant array of total term frequencies.
				totalTermFrequencyInCorpus[i] += article.getTermFrequencyInCurrentDocument()[i];	
			}
			
		}
		tf.setTotalTermFrequencyInCorpus(totalTermFrequencyInCorpus);
		return new Tuple2<Query,TotalTF>(key,tf);
	}

}