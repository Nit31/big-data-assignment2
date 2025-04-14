from cassandra.cluster import Cluster


def create_keyspace_and_tables(session):
    # Create keyspace
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS bigdata WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        };
        """
    )

    # Switch to the keyspace
    session.set_keyspace("bigdata")

    # Create tables
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS df (
            "token" text PRIMARY KEY,
            n_docs int
        );
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS tf (
            document_id text,
            "token" text,
            frequency int,
            PRIMARY KEY (document_id, "token")
        );
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS docs (
            id text PRIMARY KEY,
            topic text,
            text text,
            len int
        );
        """
    )

    # Optimize the tables
    session.execute(
        """
        CREATE INDEX IF NOT EXISTS tf_token_idx ON tf ("token");
        """
    )


def main():
    # Ensure we use the proper contact point as defined in docker-compose.yml
    cluster = Cluster(["cassandra-server"])
    session = cluster.connect()
    create_keyspace_and_tables(session)
    print("Keyspace and tables created successfully in Cassandra.")

    cluster.shutdown()


if __name__ == "__main__":
    main()
