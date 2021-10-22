import os
import pymongo
import dotenv

dotenv.load_dotenv('.env')

data = {'1': 1, '2': 2, '3': 3}

client = pymongo.MongoClient(os.getenv('DATA_BASE'))
db = client['cian_moscow']
collection = db['cian_moscow']
collection.insert_one(data)
print(1)
