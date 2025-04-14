import sys

sys.path.insert(0, "libs.zip")
from cassandra.cluster import Cluster


def insert_tf(session, doc_id, token, count):
    query = """INSERT INTO tf (document_id, "token", frequency) VALUES (%s, %s, %s)"""
    session.execute(query, (doc_id, token, count))


def main():
    cluster = Cluster(["cassandra-server"])
    session = cluster.connect("bigdata")

    current_doc = None
    current_token = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        doc_id, token, count = line.split("\t", 2)
        count = int(count)

        # Aggregate counts for the token in the same document
        if current_doc == doc_id and current_token == token:
            current_count += count
        else:
            # If the docement or token changes, insert the previous count
            if current_doc is not None and current_token is not None:
                insert_tf(session, current_doc, current_token, current_count)
                print(f"{current_doc}\t{current_token}\t{current_count}")

            current_doc = doc_id
            current_token = token
            current_count = count

    # Output the last token
    if current_doc is not None and current_token is not None:
        insert_tf(session, current_doc, current_token, current_count)
        print(f"{current_doc}\t{current_token}\t{current_count}")


if __name__ == "__main__":
    main()
