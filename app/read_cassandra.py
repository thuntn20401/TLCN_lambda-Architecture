import datetime
import logging
import sys
import time
from datetime import timedelta

from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import pandas as pd

try:
    cluster = Cluster(["localhost"], port="9042")
    session = cluster.connect("stackoverflow")
    print("Connect cassandra")
except Exception as e:
    print("Couldn't connect to the cassandra cluster")
    logging.error(e)
    sys.exit(1)
