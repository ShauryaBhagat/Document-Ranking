package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;


import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWithQuery;


import uk.ac.gla.dcs.bigdata.studentfunctions.TermCountCalculator;

import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

/**
 * Converts a NewsArticle object to ArticleWithQuery object 
 *
 */

public class ArticleWithQueryMap implements FlatMapFunction <NewsArticle, ArticleWithQuery>{
	
	private static final long serialVersionUID = 5685602376695019352L;
	
	private transient TextPreProcessor processor;
	private transient TermCountCalculator countCalculator;
	
	Broadcast<List<Query>> queriesBV;
	LongAccumulator allDocumentLength;
	
	public ArticleWithQueryMap() {}	
	
	public ArticleWithQueryMap(Broadcast<List<Query>> queriesBV, LongAccumulator allDocumentLength) {
		super();
		this.queriesBV = queriesBV;
		this.allDocumentLength = allDocumentLength;
	}



	public Iterator<ArticleWithQuery> call(NewsArticle news) throws Exception {
		
		if (processor==null) processor = new TextPreProcessor();
		if (countCalculator==null) countCalculator = new TermCountCalculator();
		
		//Title is extracted and processed by TextPreProcessor. Then the current document length count is incremented by the size of the 
		// list of strings of the title.
		String title = news.getTitle();
		List<String> titleProcessed = processor.process(title);
		int count = titleProcessed.size();
		
		//Content items extracted as a list for further processing.
		List<ContentItem> item = news.getContents();
		List<ContentItem> contentItemList = new ArrayList<ContentItem>();
		
		//Counter to maintain the maximum five contents of subtype = paragraph 
		int maxContents =0;
		
		//Counter to iterate through the content item list
		int i =0;
		
		//The while loop must stop when either the maximum number of contents are processed or the content item list is exhausted.
		while ((maxContents < 5) && (i<item.size())) {
		
			if((item.get(i).getSubtype() != null) && (item.get(i).getSubtype().equalsIgnoreCase("paragraph") )) {
				
				maxContents +=1;
				
				List<String> contentProcessed = processor.process(item.get(i).getContent());
				
				//count of document length is incremented by the size of the list of strings of the current content item's content.
				count += contentProcessed.size();
				
				item.get(i).setContent(contentProcessed.toString());
				
				contentItemList.add(item.get(i));
			}
			i+=1;
		}
		
		news.setContents(contentItemList);
		
		//The following processing of query term frequency should only happed when the count of the current document length is not zero.
		if (count != 0) {
			
			//accumulator is updated.
			allDocumentLength.add(count);
			
			List<Query> queryList = queriesBV.value();
			
			List<ArticleWithQuery> articleWithQueryList = new ArrayList<ArticleWithQuery>(queryList.size());
			
			//The following nested for loops are to iterate through all the queries in the query list and all the query terms in each of the query.
			//The query term frequency is calculated by calculating the number of times the query term string appears in the title or the 
			// list of 5 contents extracted above.
			
			for(Query q: queryList) {
				
				List<String> queryTerms = q.getQueryTerms();
			
				short[] queryTermCounts = new short[queryTerms.size()];
				
				//pointer to point the correct index of the array queryTermCounts where the count of term frequency needs to be placed.
				int queryTermCounter = 0;
				
				for(String s: queryTerms) {
					
					//term frequency contributed by the title.
					short tf = countCalculator.termCountCalc(s, titleProcessed);
					
					for (ContentItem it: contentItemList) {
						
						List<String> contentProcessed = processor.process(it.getContent());
						//term frequency contributed by the contents.
						tf += countCalculator.termCountCalc(s, contentProcessed);
						
					}
					
					queryTermCounts[queryTermCounter] = tf;
					
					queryTermCounter += 1;
					
				}
				
				//Creating an object of type ArticleWithQuery and setting the characteristics
				ArticleWithQuery ArticleWithQuery = new ArticleWithQuery();
				
				ArticleWithQuery.setArticle(news);
				ArticleWithQuery.setDoc_len(count);
				ArticleWithQuery.setQuery(q);
				ArticleWithQuery.setTermFrequencyInCurrentDocument(queryTermCounts);
				
				//Adding the object to the list
				articleWithQueryList.add(ArticleWithQuery);
				
			}		
			//return the new list of ArticleWithQuery iterator once all the queries is processed for the NewsArticle. 
			return articleWithQueryList.iterator();
			
		}
		//return empty iterator when the count of current document length is zero so that such articles can be filtered out.
		else {
			List<ArticleWithQuery> articleWithQueryList = new ArrayList<ArticleWithQuery>(0);
			
			return articleWithQueryList.iterator();
			
		}
		
	}

}
