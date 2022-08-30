from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "wjddks"))

def create_person(tx, name):
    tx.run("CREATE (a:Person {name: $name})", name=name)

def create_friend_of(tx, name, friend):
    tx.run("MATCH (a:Person) WHERE a.name = $name "
           "CREATE (a)-[:KNOWS]->(:Person {name: $friend})",
           name=name, friend=friend)

with driver.session() as session:
    session.write_transaction(create_person, "Alice")
    session.write_transaction(create_friend_of, "Alice", "Bob")
    session.write_transaction(create_friend_of, "Alice", "Carl")

def get_friends_of(tx, name):
    friends = []
    result = tx.run("MATCH (a:Person)-[:KNOWS]->(f) "
                    "WHERE a.name = $name "
                    "RETURN f.name AS friend", name=name)
    for record in result:
        friends.append(record["friend"])
    return friends

with driver.session() as session:
    friends = session.read_transaction(get_friends_of, "Alice")
    for friend in friends:
        print(friend)
        
driver.close()