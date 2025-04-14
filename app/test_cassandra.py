from cassandra.cluster import Cluster

cluster = Cluster(["cassandra-server"])
session = cluster.connect("bigdata")

print("First 5 rows in tf table:")
rows = session.execute("SELECT * FROM tf")
for row in rows[:5]:
    print(f"\t{row}")

print("-" * 80)

print("First 5 rows in df table:")
rows = session.execute("""SELECT * FROM df""")
for row in rows[:5]:
    print(f"\t{row}")

print("-" * 80)

print("Example from docs table:")
rows = session.execute("SELECT * FROM docs")
for row in rows.one():
    print(f"\t{row}")
