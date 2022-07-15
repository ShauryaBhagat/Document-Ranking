package uk.ac.gla.dcs.bigdata.apps;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWithQuery;
import uk.ac.gla.dcs.bigdata.studentstructures.TotalTF;

import uk.ac.gla.dcs.bigdata.studentfunctions.TotalTFInCorpus;
import uk.ac.gla.dcs.bigdata.studentfunctions.DPHScorerMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.ArticleWithQueryMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.ArticleToQuery;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("SPARK_MASTER");
		if (sparkMasterDef==null) {
			File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
			System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
			sparkMasterDef = "local[2]"; // default is local mode with two executors
		}
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("BIGDATA_QUERIES");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("BIGDATA_NEWS");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		String out = System.getenv("BIGDATA_RESULTS");
		String resultsDIR = "results/";
		if (out!=null) resultsDIR = out;
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(new File(resultsDIR).getAbsolutePath());
			}
		}
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(resultsDIR).getAbsolutePath()+"/SPARK.DONE")));
			writer.write(String.valueOf(System.currentTimeMillis()));
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}

	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		
		// Accumulator to calculate the total number of document length
		LongAccumulator allDocumentLength = spark.sparkContext().longAccumulator();
		
		//Dataset queries is collected as a list so that it can be broadcasted to the flatMap function
		List<Query> queryList = queries.collectAsList();
		Broadcast<List<Query>> queriesBV = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryList);		
		
		//flatMap function maps the items of dataset of type NewsArticle to a data set of type ArticleWithQuery.
		//ArticleWithQuery is a new structure defined. Each object of this class will have a news article object, the length of that document,
		// a query object and the term frequency array of each of the query terms in that document.
		//This flatMap function also accumulates the allDocumentLength accumulator.
		//flatMap functions is used because it filters out those news articles which neither have text in their titles nor in the contents.
		Dataset<ArticleWithQuery> articleWithQuery = news.flatMap(new ArticleWithQueryMap(queriesBV, allDocumentLength), Encoders.bean(ArticleWithQuery.class));
		
		//As spark functions in a lazy manner the above dataset is collected as list so that it triggers the map function and accumulator is updated
		// with the required value.
		List<ArticleWithQuery> newsList= articleWithQuery.collectAsList();
		
		// As the map function maps every news article object to all the queries, totalDocsInCorpus will be the count of the mapped dataset
		// divided by the size of the queries dataset.
		// eg. if there are 5000 rows of newsArticle and 3 rows of queries, the map function returns 5000 * 3 = 15000 rows, but totalDocsInCorpus 
		// is supposed to be 5000.
		long totalDocsInCorpus = newsList.size()/queryList.size();
		
		// using the accumulator's value averageDocumentLengthInCorpus is calculated.
		double averageDocumentLengthInCorpus = (allDocumentLength.value().doubleValue())/ (totalDocsInCorpus);
		
		// totalDocsInCorpusBV and averageDocumentLengthInCorpusBV are broadcasted for the mapGroups function.
		Broadcast<Long> totalDocsInCorpusBV = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalDocsInCorpus);
		Broadcast<Double> averageDocumentLengthInCorpusBV = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(averageDocumentLengthInCorpus);
		
		//The articleWithQuery Dataset is grouped by query object as keys for simpler and more efficient calculations.
		KeyValueGroupedDataset<Query, ArticleWithQuery> articleByQuery = articleWithQuery.groupByKey(new ArticleToQuery(), Encoders.bean(Query.class));
		
		//This mapGroups function maps all the individual term frequencies of query terms from articleWithQuery Dataset to a dataset of type 
		// tuple2 having the query and its corresponding totalTermFrequencyInCorpus array for all the query's terms.
		Dataset<Tuple2<Query,TotalTF>> queryAndTotalTF = articleByQuery.mapGroups(new TotalTFInCorpus(), Encoders.tuple(Encoders.bean(Query.class), Encoders.bean(TotalTF.class)));
		
		//The above output is collected as a list so that it can be broadcasted to the mapGroups function.
		List<Tuple2<Query,TotalTF>> queryAndTotalTFList = queryAndTotalTF.collectAsList();
		Broadcast<List<Tuple2<Query,TotalTF>>> queryAndTotalTFBV = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryAndTotalTFList);
		
		//This mapGroups function maps the grouped dataset articleByQuery to the desired output dataset of type DocumentRanking.
		//This calculates DPH scores for all newsArticles for all query terms and forms a list of RankedResults.
		//This list is further processed to remove the near duplicate documents.
		Dataset<DocumentRanking> docrank =  articleByQuery.mapGroups(new DPHScorerMap(totalDocsInCorpusBV, averageDocumentLengthInCorpusBV, queryAndTotalTFBV), Encoders.bean(DocumentRanking.class));
		
		//The final output from above mapFroups function is collected as list and returned by this function
		List<DocumentRanking> docRankList = docrank.collectAsList();
				
//		for(DocumentRanking dr :docRankList) {
//			System.out.println(dr.toString());
//		}
		
		return docRankList; 
	}
	
	
}
