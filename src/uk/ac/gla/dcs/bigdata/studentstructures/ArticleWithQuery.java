package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/**
 * Represents a news article with its document length, a query object and an array of the query terms frequency in this article  
 *
 */

public class ArticleWithQuery implements Serializable {
	
	private static final long serialVersionUID = 7860293794078412243L;
	
	NewsArticle article;
	int doc_len;
	Query query;
	short[] termFrequencyInCurrentDocument;
	
	public ArticleWithQuery() {}

	public ArticleWithQuery(NewsArticle article, int doc_len, Query query,
			short[] termFrequencyInCurrentDocument) {
		super();
		this.article = article;
		this.doc_len = doc_len;
		this.query = query;
		this.termFrequencyInCurrentDocument = termFrequencyInCurrentDocument;
	}

	public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public int getDoc_len() {
		return doc_len;
	}

	public void setDoc_len(int doc_len) {
		this.doc_len = doc_len;
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public short[] getTermFrequencyInCurrentDocument() {
		return termFrequencyInCurrentDocument;
	}

	public void setTermFrequencyInCurrentDocument(short[] termFrequencyInCurrentDocument) {
		this.termFrequencyInCurrentDocument = termFrequencyInCurrentDocument;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

		

}
