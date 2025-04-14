import math
import re
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Regular expression to capture word tokens
token_pattern = re.compile(r"\b\w+\b")


def tokenize_text(text):
    # Convert to lowercase and split into tokens
    return token_pattern.findall(text.lower())


def search_bm25(query, k1=1.2, b=0.75):
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("BM25 Search")
        .config("spark.cassandra.connection.host", "cassandra-server")
        .config("spark.cassandra.auth.username", "cassandra")
        .config("spark.cassandra.auth.password", "cassandra")
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0")
        .getOrCreate()
    )

    query_tokens = tokenize_text(query)

    # If query is empty, return empty list
    if not query_tokens:
        spark.stop()
        return []

    # Compute corpus statistics from the docs table
    docs_all = spark.read.format("org.apache.spark.sql.cassandra").options(table="docs", keyspace="bigdata").load()
    N = docs_all.count()
    dl_avg = docs_all.select(avg("len")).first()[0]

    # Get document frequencies for query tokens
    df_data = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="df", keyspace="bigdata")
        .load()
        .filter(col("token").isin(query_tokens))
    )

    # Dictionary of token: n_docs
    token_df_map = {row["token"]: row["n_docs"] for row in df_data.collect()}

    # If none of the query tokens are in the corpus, return None
    if not token_df_map:
        spark.stop()
        return (None, None)

    # Get term frequencies for documents containing query tokens
    tf_data = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="tf", keyspace="bigdata")
        .load()
        .filter(col("token").isin(query_tokens))
    )

    # Get all document IDs that contain at least one query token
    document_ids = tf_data.select("document_id").distinct().rdd.flatMap(lambda x: x).collect()

    # If no documents contain any query token, return None
    if not document_ids:
        spark.stop()
        return (None, None)

    # Get document info
    docs_data = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="docs", keyspace="bigdata")
        .load()
        .filter(col("id").isin(document_ids))
    )

    # Create a dictionary document_id: (len, topic, text)
    doc_info = {row["id"]: (row["len"], row["topic"], row["text"]) for row in docs_data.collect()}

    # Broadcast necessary data for efficient lookups
    doc_info_bc = spark.sparkContext.broadcast(doc_info)
    token_df_bc = spark.sparkContext.broadcast(token_df_map)
    N_bc = spark.sparkContext.broadcast(N)
    dl_avg_bc = spark.sparkContext.broadcast(dl_avg)

    # Convert the Spark DataFrame to an RDD and map each row to a tuple of (document_id, token, frequency)
    tf_data_rdd = tf_data.rdd.map(lambda row: (row["document_id"], row["token"], row["frequency"]))

    # compute the partial BM25 score for a given term in a document
    def term_bm25(doc_id, token, freq):
        df_t = token_df_bc.value.get(token, 0)
        idf = math.log(1 + (N_bc.value - df_t + 0.5) / (df_t + 0.5))
        doc_len, doc_topic, _ = doc_info_bc.value[doc_id]
        tf_component = (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * doc_len / dl_avg_bc.value))
        return idf * tf_component

    # Compute the partial score for each document-token tuple
    doc_scores_rdd = tf_data_rdd.map(lambda x: (x[0], term_bm25(x[0], x[1], x[2])))

    # Sum up scores for each document by tokens
    aggregated_scores_rdd = doc_scores_rdd.reduceByKey(lambda a, b: a + b)

    # include the document's topic
    scored_docs_rdd = aggregated_scores_rdd.map(lambda x: (x[0], x[1], doc_info_bc.value[x[0]][1]))
    # Get the top 10 documents
    top_docs = scored_docs_rdd.takeOrdered(10, key=lambda x: -x[1])

    spark.stop()
    return top_docs, doc_info


def main():
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
    else:
        raise ValueError("Please provide a search query as an argument.")

    results, doc_info = search_bm25(query)

    print(f"\n\n\nTop 10 Documents for query: '{query}'")
    print("-" * 80)

    if results:
        for i, (doc_id, score, topic) in enumerate(results, 1):
            # Retrieve document text from the dictionary
            doc_text = doc_info[doc_id][2] if doc_id in doc_info else "N/A"
            print(f"{i}. Document ID: {doc_id}")
            print(f"   Topic: {topic}")
            print(f"   Score: {score:.4f}")
            # Print first 100 characters of the text
            print(f"   Text: {doc_text[:100]}...")
            print("-" * 80)
    else:
        print("No matching documents found.")
    print("\n" * 3)


if __name__ == "__main__":
    main()
