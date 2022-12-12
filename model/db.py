import pymongo
import pandas as pd
from dataclasses import dataclass

@dataclass
class MDB:
    """Connect Mongodb

    #Property
    host: Mongodb host name
    port: Mongodb port
    user: User name
    db_name: Database name
    client: Mongodb Client class
    db: Connected database    
    """
    host: str
    port: str
    user: str
    passowrd: int
    db_name: str
    
    def __post_init__(self):
        self.client = pymongo.MongoClient(host=self.host, 
                                          port=self.port)
        self.db = self.client[self.db_name]

        self.col = {
            'user':self.db['user'],
            'newsfeed':self.db['newsfeed'],
            'topic':self.db['topic'],
            'user_user':self.db['user_user'],
            'newsfeed_newsfeed':self.db['newsfeed_newsfeed'],
            'user_newsfeed':self.db['user_newsfeed'],
            'user_like':self.db['user_like'],
            'topic_link':self.db['topic_link'],
            'user_create':self.db['user_create']
            }
    def inser_data(self, collection:str, data_list:list):
        for data in data_list:
            if len(self.search_data(collection, data,1))!=0:
                continue
            self.col[collection].insert_one(data)
        pass

    def search_data(self, collection:str, data_info:dict, limit:int=0):
        if data_info:
            try:
                [x for x in self.col[collection].find(data_info).limit(limit)]
            except Exception as e:
                print(e)
            return [x for x in self.col[collection].find(data_info).limit(limit)]

        return [x for x in self.col[collection].find().limit(limit)]


if __name__ == "__main__":
    uri = 'mongodb://localhost:27017'
    db_name = 'recommend'
    mdb = MDB(uri, db_name=db_name)
    mdb.inser_data('user', [{'userId': 1, 'gender': 1, 'displayName': '深深。', 'meetPreference': 'unlimited', 'birthday': 1053043200}])