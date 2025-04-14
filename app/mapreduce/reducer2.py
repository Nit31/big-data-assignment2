import sys

sys.path.insert(0, "libs.zip")
from cassandra.cluster import Cluster


def insert_df(session, token, count):
    query = """INSERT INTO df ("token", n_docs) VALUES (%s, %s)"""
    session.execute(query, (token, count))


def main():
    cluster = Cluster(["cassandra-server"])
    session = cluster.connect("bigdata")
    current_token = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        token, count = line.split("\t", 1)
        count = int(count)

        # Aggregate counts for the current token
        if current_token == token:
            current_count += count
        else:
            # If the token changes, output the result for the previous token
            if current_token is not None:
                insert_df(session, current_token, current_count)
                print(f"{current_token}\t{current_count}")
            current_token = token
            current_count = count

    # Output the count for the last token processed
    if current_token is not None:
        insert_df(session, current_token, current_count)
        print(f"{current_token}\t{current_count}")


if __name__ == "__main__":
    main()
