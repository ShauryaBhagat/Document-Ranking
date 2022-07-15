package uk.ac.gla.dcs.bigdata.studentfunctions;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Iterator;
import org.apache.spark.api.java.function.MapGroupsFunction;


import org.apache.spark.broadcast.Broadcast;


import scala.Tuple2;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;

import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWithQuery;
import uk.ac.gla.dcs.bigdata.studentstructures.TotalTF;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

/**
 * This mapGroups function maps the grouped dataset of <key, values> pair of type <Query, ArticleWithQuery> to a single object of type
 * DocumentRanking.
 *
 */

public class DPHScorerMap implements MapGroupsFunction<Query, ArticleWithQuery, DocumentRanking>{
	
	private static final long serialVersionUID = 5685602376695019352L;
	
	Broadcast<Long> totalDocsInCorpusBV;
	Broadcast<Double> averageDocumentLengthInCorpusBV;
	Broadcast<List<Tuple2<Query,TotalTF>>> queryAndTotalTFBV;
	
	
	public DPHScorerMap(Broadcast<Long> totalDocsInCorpusBV, Broadcast<Double> averageDocumentLengthInCorpusBV, Broadcast<List<Tuple2<Query,TotalTF>>> queryAndTotalTFBV) {
		super();
		this.totalDocsInCorpusBV = totalDocsInCorpusBV;
		this.averageDocumentLengthInCorpusBV = averageDocumentLengthInCorpusBV;
		this.queryAndTotalTFBV = queryAndTotalTFBV;
	}


	public DocumentRanking call(Query key, Iterator<ArticleWithQuery> values) throws Exception {	
		
		List<RankedResult> rankedResultList = new ArrayList<RankedResult>();
		List<Tuple2<Query,TotalTF>> queryAndTotalList = queryAndTotalTFBV.value();
		
		//The below 2 values will be constant irrespective of the particular document
		double averageDocumentLengthInCorpus = averageDocumentLengthInCorpusBV.value();
		long totalDocsInCorpus = totalDocsInCorpusBV.value();
		
		//For all ArticleWithQuery objects under a Query as key, the DPH is caluclated and a list of RankedResult rankedResultList is created.
		while (values.hasNext()) {
			ArticleWithQuery article = values.next();
			
			int currentDocumentLength = article.getDoc_len();
			
			int[] totalTF = new int[key.getQueryTermCounts().length];
			
			//The following for loop is to iterate through the braodcasted list of total term frequencies and find the one matching the current
			// key value which is the current Query object. This is found by matching the original query string.
			for(int i=0; i<queryAndTotalList.size(); i++) {
				
				if(key.getOriginalQuery().equalsIgnoreCase(queryAndTotalList.get(i)._1.getOriginalQuery())) {
					
					totalTF = queryAndTotalList.get(i)._2.getTotalTermFrequencyInCorpus();
					
				}
				
			}
			
			int querySize = article.getTermFrequencyInCurrentDocument().length;
			
			double score =0.0;
			
			//The following loop is to iterate through all the query terms in the current query and calculate average DPH score.
			for (int i=0; i<querySize; i++)
			{
				short termFrequencyInCurrentDocument = article.getTermFrequencyInCurrentDocument()[i];
				
				//If the term does not occur in the document the DPH score will remain as 0.
				if (termFrequencyInCurrentDocument !=0) {
					
					int totalTermFrequencyInCorpus = totalTF[i];
					
					score += DPHScorer.getDPHScore(termFrequencyInCurrentDocument, 
							totalTermFrequencyInCorpus, 
							currentDocumentLength, 
							averageDocumentLengthInCorpus, 
							totalDocsInCorpus);
				}
			}
			
			//The average of the DPH scores for all <document, term> pairs is the DPH score for the <document, query>.
			if (score !=0.0) {
				score = score / querySize;
				
			}
			
			//An object of type RankedResult is created and characteristics are set.   
			RankedResult rr = new RankedResult();
			rr.setArticle(article.getArticle());
			rr.setDocid(article.getArticle().getId());
			rr.setScore(score);
			
			//The object is added to the list of RankedResult objects.
			rankedResultList.add(rr);
			
		}
		
		//The above list is sorted (by default in ascending order) and then reversed to find out the top 10 documents having highest DPH scores.
		Collections.sort(rankedResultList);
		Collections.reverse(rankedResultList);
		
		//results list will contain the final 10 RankedResult objects which will be added to the DocumentRanking object and returned by the function.
		List<RankedResult> results = new ArrayList<RankedResult>();
		
		//Initially the top 10 objects of the rankedResultList is taken into the results.
		results = rankedResultList.subList(0, 10);
		
		//The following is the logic designed to remove the near duplicate documents from the list of RankedResult objects. 
		
		//This is the pointer to fetch further objects after index 10 from the rankedResultList.
		int extra = 10;
		
		//For loop to iterate through the top 10 RankedResult objects.
		for (int i =0; i< 10; i++) {
			//initial object selected
			RankedResult rr = results.get(i);
			
			String title1 = rr.getArticle().getTitle();
			//If the title has no text then it is considered unique and document is kept in the final result list
			if (title1 == null) continue;
			
			//For loop to iterate through the remaining objects starting from the next of the initial till the end of the list
			for (int j=i + 1; j<results.size(); j++) {
				
				String title2 = results.get(j).getArticle().getTitle();
				if (title2 == null) continue;
				
				//The textual distance is calculated between the 2 titles
				double dist = TextDistanceCalculator.similarity(title1, title2);
				
				//as per the provided logic if textual distance is less than 0.5, the document with higher DPH is to be kept and the other removed.
				if (dist<0.5) {
					RankedResult rmv = results.remove(j);
				}
				
			}
			
			//That many number of objects are added from the rankedResultList as the number of RankedResult objects that are removed
			// from the list above. 
			//This is to ensure that the final results list has 10 documents even after removal of the redundant documents.
			while (results.size()!=10) {
				results.add(rankedResultList.get(extra));
				extra +=1;
			}
			
		}
				
		//The final DocumentRanking object is created and returned where query is set from the key value and the results list of RankedResults.
		DocumentRanking dr = new DocumentRanking();
		dr.setQuery(key);
		dr.setResults(results);
		return dr;
	}

}
