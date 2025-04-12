from cassandra.cluster import Cluster

def drop_keyspace():
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()
    session.execute("DROP KEYSPACE bigdata;")
    print("Keyspace bigdata dropped successfully.")
    cluster.shutdown()

if __name__ == "__main__":
    drop_keyspace()