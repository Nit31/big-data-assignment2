import sys
import zipimport

sys.path.insert(0, "libs.zip")
from cassandra.cluster import Cluster


def insert_stats(session, key, value):
    query = """INSERT INTO stats (key, value) VALUES (%s, %s)"""
    session.execute(query, (key, value))


def main():
    cluster = Cluster(["cassandra-server"])
    session = cluster.connect("bigdata")

    total_docs = 0
    total_length = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) != 2:
            continue

        doc_count_str, doc_length_str = parts
        try:
            doc_count = int(doc_count_str)
            doc_length = int(doc_length_str)
        except ValueError:
            continue

        total_docs += doc_count
        total_length += doc_length

    # Calculate the average document length if total_docs is not zero
    dl_avg = total_length / total_docs if total_docs > 0 else 0

    # Output the total document count and average document length
    if total_docs > 0:
        insert_stats(session, "N", total_docs)
        print(f"N\t{total_docs}")
    else:
        print("N\t0")

    if dl_avg > 0:
        insert_stats(session, "dl_avg", dl_avg)
        print(f"dl_avg\t{dl_avg}")
    else:
        print("dl_avg\t0")


if __name__ == "__main__":
    main()
