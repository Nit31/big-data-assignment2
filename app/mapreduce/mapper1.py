import re
import sys

sys.path.insert(0, "libs.zip")
from cassandra.cluster import Cluster

# Regular expression to capture word tokens
token_pattern = re.compile(r"\b\w+\b")


def insert_docs(session, doc_id, title, text, word_count):
    query = """INSERT INTO docs (id, topic, text, len) VALUES (%s, %s, %s, %s)"""
    session.execute(query, (doc_id, title, text, word_count))


def tokenize_text(text):
    # Convert to lowercase and split into tokens
    return token_pattern.findall(text.lower())


def main():
    # Connect to Cassandra
    cluster = Cluster(["cassandra-server"])
    session = cluster.connect("bigdata")

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        # Split line into document id, title, and text
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue

        doc_id, title, text = parts

        # Tokenization
        tokens = tokenize_text(text)

        # Calculate the document length and put the document into the database
        word_count = len(tokenize_text(text))
        insert_docs(session, doc_id, title, text, word_count)

        for token in tokens:
            print(f"{doc_id}\t{token}\t1")


if __name__ == "__main__":
    main()
