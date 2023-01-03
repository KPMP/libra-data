from distutils.core import setup
from ..redcap import Redcap
import mongomock
import unittest
from bson.objectid import ObjectId
import datetime


class Test_Redcap(unittest.TestCase):
    def test_redcap(self):
        collection = mongomock.MongoClient().dataLake.redcap
        object = {
            "_id": ObjectId("609d30f9b1e6a80049a273bf"),
            "data": "stuff",
            "created_at": datetime.datetime(2021, 5, 13, 14, 0, 25, 312000),
        }

        collection.insert_one(object)

        redcap = Redcap()
        redcap.collection = collection
        redcap_data = redcap.get_redcap_data()
        assert len(redcap_data) == 1
        print("mocked data", redcap_data)
        object_two = {
            "_id": ObjectId("709d30f9b1e6a80049a273bf"),
            "data": "stuff",
            "created_at": datetime.datetime(2021, 5, 13, 14, 0, 25, 312000),
        }
        collection.insert_one(object_two)
        redcap_data = redcap.get_redcap_data()

        assert len(redcap_data) == 2
