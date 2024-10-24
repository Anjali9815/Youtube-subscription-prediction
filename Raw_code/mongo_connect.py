con = "mongodb+srv://anjalijha1507:U54OU4PFxPYlVc4S@youtubedata.shzzp.mongodb.net/?retryWrites=true&w=majority&appName=YoutubeData"

from pymongo import MongoClient
import certifi
import warnings

warnings.filterwarnings("ignore")

MONGO_CONNECTION_STRING = con

def connect_to_mongodb(MONGO_CONNECTION_STRING):
    try:
        # Create a MongoClient instance with CA bundle specified
        client = MongoClient(MONGO_CONNECTION_STRING, tls=True, tlsCAFile=certifi.where())

        # Attempt to get server information to confirm connection
        client.server_info()  # Forces a call to the server
        print("Successfully connected to MongoDB.")

        # Access a specific database (replace 'test' with your database name)
        db = client['Project1']
        
        return client, db

    except Exception as e:
        print("Error connecting to MongoDB:", e)
        return None, None

def main():
    client, db = connect_to_mongodb(MONGO_CONNECTION_STRING)
    # if db is not None:  # Check explicitly if db is not None
    # #     # Access a specific collection (replace 'your_collection' with your collection name)
    #     collection = db['youtube_channel_data']

    #     # Example operation: Find one document
    #     document = collection.find_one()
    #     print('Sample Document:', document)

    #     # Example operation: Fetch all documents
    #     all_documents = list(collection.find())
    #     print('AllDocuments:')
    #     for doc in all_documents:
    #         print(doc)

    #     # Example operation: Insert a new document
    #     new_document = {
    #         'name': 'Example',
    #         'value': 123
    #     }
    #     insert_result = collection.insert_one(new_document)
    #     print('Inserted Document ID:', insert_result.inserted_id)

    #     # Close the connection
    #     client.close()
    # # else:
    # #     print("Database connection was not established.")

if __name__ == "__main__":
    main()
