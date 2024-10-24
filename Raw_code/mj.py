# from pymongo import MongoClient
# from pymongo.server_api import ServerApi

# uri = "mongodb+srv://anjalijha1507:U54OU4PFxPYlVc4S@youtubedata.shzzp.mongodb.net/?retryWrites=true&w=majority&appName=YoutubeData"

# # Create a new client and connect to the server
# client = MongoClient(uri, server_api=ServerApi('1'), tls=True, tlsAllowInvalidCertificates=True)

# # Send a ping to confirm a successful connection
# try:
#     client.admin.command('ping')
#     print("Pinged your deployment. You successfully connected to MongoDB!")
# except Exception as e:
#     print(e)

import names

fname = names.get_first_name()
print(fname)

