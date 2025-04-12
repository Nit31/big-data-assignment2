import re
import sys
import zipimport

sys.path.insert(0, "libs.zip")
from cassandra.cluster import Cluster

token_pattern = re.compile(r"\b\w+\b")


def tokenize_text(text):
    # Convert to lowercase and split into tokens
    return token_pattern.findall(text.lower())


def insert_docs(session, doc_id, title, text, word_count):
    query = """INSERT INTO docs (id, topic, text, len) VALUES (%s, %s, %s, %s)"""
    session.execute(query, (doc_id, title, text, word_count))


def main():
    # Connect to Cassandra
    cluster = Cluster(["cassandra-server"])
    session = cluster.connect("bigdata")

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue

        doc_id, title, text = parts

        # Calculate the document length and put the document into the database
        word_count = len(tokenize_text(text))
        insert_docs(session, doc_id, title, text, word_count)

        print(f"1\t{word_count}")


if __name__ == "__main__":
    main()
