package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWithQuery;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/**
 * Maps a ArticleWithQuery object to its query object
 *
 */

public class ArticleToQuery implements MapFunction<ArticleWithQuery,Query>{
	
	private static final long serialVersionUID = 525739182048149914L;
	
	public Query call (ArticleWithQuery news) throws Exception {
		
		return news.getQuery();
	}

}
