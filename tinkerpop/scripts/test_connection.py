import os

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

g = traversal().withRemote(
    DriverRemoteConnection(f"ws://localhost:8182/gremlin", "g")
)

results = g.V().count().next()

print(f"Connected to Neptune! There are {results} vertices in the database")
