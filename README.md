# Document-Ranking
A Batch-based text search and filtering pipeline in Apache Spark

The core goal of this pipeline is to take in a large set of text documents and a set of user defined queries, then for each query, rank the text documents by relevance for that query, as well as filter out any overly similar documents in the final ranking.
