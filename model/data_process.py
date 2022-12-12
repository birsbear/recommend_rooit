import sys
import logging
import tomllib
import pandas as pd
import numpy as np
from model.db import MDB
from dataclasses import dataclass


@dataclass
class Data:
    data_path: str
    db_select: str
    config_path: str
    def __post_init__(self):
        with open(self.config_path, 'rb') as f:
            config = tomllib.load(f)
        match self.db_select:
            case "mongo":
                host = config['database']['mongo']["host"]
                user_account = config['database']['mongo']["user_name"]
                user_password = config['database']['mongo']["user_password"]
                port = int(config['database']['mongo']['port'])
                db_name = config['database']['mongo']['db']
                self.db = MDB(host, port, user_account, user_password, db_name)

    def load_data(self):
        df = pd.read_excel(self.data_path, sheet_name=None)
        user, newsfeed, user_like = df['User'], df['NewsFeed'], df['UserLike']
        user.rename(columns={'id':'userId'}, inplace=True)
        user = user.reset_index(drop=True).reset_index().rename(columns={'index':'userIndex'})
        user[['userIndex','userId','gender']] = user[['userIndex','userId','gender']].astype(str)
        user['birthday'] = user.apply(lambda x : x['birthday'].timestamp(), axis=1).astype(int).astype(str)
        

        newsfeed.rename(columns={'id':'newsFeedId'}, inplace=True)
        newsfeed[['newsFeedId','status','userId','likeCount','readCount']] = newsfeed[['newsFeedId','status','userId','likeCount','readCount']].astype(str)
        newsfeed['createdAt'] = newsfeed.apply(lambda x : x['createdAt'].timestamp(), axis=1).astype(int).astype(str)


        user_like.rename(columns={'ID':'relationshipId'}, inplace=True)
        user_like[['relationshipId','userId','newsFeedId']] = user_like[['relationshipId','userId','newsFeedId']].astype(str)
        user_like = user[['userIndex','userId']].merge(user_like, how='left', on='userId')
        topic_link = newsfeed[['newsFeedId','topic']]
        
        user_create_newsfeed = newsfeed[['userId','newsFeedId']]

        user_newsfeed_matrix, user_user_matrix, newsfeed_newsfeed_matrix = self.create_simlarty_matrix(user, newsfeed, user_like)
        
        
        match self.db_select:
            case "mongo":
                self.write_data_to_db('user',user)
                self.write_data_to_db('newsfeed',newsfeed)
                self.write_data_to_db('user_like',user_like)        
                self.write_data_to_db('topic_link',topic_link)
                self.write_data_to_db('user_create',user_create_newsfeed)
                self.write_data_to_db('user_newsfeed',user_newsfeed_matrix)
                self.write_data_to_db('user_user',user_user_matrix)
                self.write_data_to_db('newsfeed_newsfeed',newsfeed_newsfeed_matrix)

        pass

    def create_simlarty_matrix(self, user, newsfeed, user_like):
        user_shape = user.shape[0]
        newsfeed_shape = newsfeed.shape[0]
        user_newsfeed_matrix = self.create_user_newsfeed_matrix(user_shape, newsfeed_shape, user_like)
        newsfeed_newsfeed_matrix = self.create_newsfeed_newsfeed_matrix(newsfeed_shape, user_newsfeed_matrix)
        user_user_matrix = self.create_user_user_matrix(user_shape, user_newsfeed_matrix)

        user_newsfeed_df = self.matrix_to_dataframe(user_newsfeed_matrix, 'user', 'newsfeed')
        user_user_df = self.matrix_to_dataframe(user_user_matrix, 'user', 'user')
        newsfeed_newsfeed_df = self.matrix_to_dataframe(newsfeed_newsfeed_matrix, 'newsfeed', 'newsfeed')

        return user_newsfeed_df, user_user_df, newsfeed_newsfeed_df
    def create_user_newsfeed_matrix(self, user_shape, newsfeed_shape, user_like):
        user_newsfeed_matrix = np.array([[0]*newsfeed_shape]*user_shape)
        for _, row in user_like.iterrows():
            user_id, newsfeed_id = row[['userIndex','newsFeedId']]
            user_newsfeed_matrix[int(user_id)][int(newsfeed_id)-1] = 1

        return user_newsfeed_matrix

    def create_newsfeed_newsfeed_matrix(self, newsfeed_shape, user_newsfeed_matrix):
        newsfeed_newsfeed_matrix = np.array([[0.0]*newsfeed_shape]*newsfeed_shape)

        for row in range(newsfeed_shape):
            for column in range(row+1,newsfeed_shape):
                row_newsfeed = user_newsfeed_matrix[:, row]
                column_newsfeed = user_newsfeed_matrix[:, column]
                simlarty = sum((row_newsfeed+column_newsfeed)==2)/sum((row_newsfeed+column_newsfeed)>0) if sum((row_newsfeed+column_newsfeed)>0) != 0 else 0
                newsfeed_newsfeed_matrix[row, column] = round(simlarty, 5)
                newsfeed_newsfeed_matrix[column, row] = round(simlarty, 5)

        return newsfeed_newsfeed_matrix

    def create_user_user_matrix(self, user_shape, user_newsfeed_matrix):
        user_user_matrix = np.array([[0.0]*user_shape]*user_shape)

        for row in range(user_shape):
            for column in range(row+1,user_shape):
                row_user = user_newsfeed_matrix[row, :]
                column_user = user_newsfeed_matrix[column, :]
                simlarty = sum((row_user+column_user)==2)/sum((row_user+column_user)>0) if sum((row_user+column_user)>0) != 0 else 0
                user_user_matrix[row, column] = round(simlarty, 5)
                user_user_matrix[column, row] = round(simlarty, 5)

        return user_user_matrix
    def matrix_to_dataframe(self, matrix, index_name, column_name):
        matrix_dict = {}
        for ind_r, row in enumerate(matrix):
            row_dict = {}
            for ind_c, column in enumerate(row):
                row_dict[f"{column_name}_{ind_c+1}"] = str(column)
            matrix_dict[f"{index_name}_{ind_r+1}"] = row_dict
        matrix_df = pd.DataFrame.from_dict(matrix_dict, orient='index')
        matrix_df = matrix_df.reset_index().rename(columns={'index':f'{index_name}'})
        return matrix_df

    def write_data_to_db(self, collection:str, dataframe:pd.DataFrame):
        data_dict = dataframe.to_dict('index')
        data_list = [data_dict[key] for key in data_dict]
        print(f'Start writing {collection} data to db...')
        self.db.inser_data(collection, data_list)
        print(f'Finish writing {collection} data to db')
        pass
    
    def get_user(self, meet_preference:str = 'unlimited'):

        
        match meet_preference :
            
            case 'male'|'female':
                user_info = {'meetPreference':meet_preference}
            case 'unlimited':
                user_info = ''
            case _:
                return "error Meet Preference"

        match self.db_select:
            case "mongo":
                user_df = pd.DataFrame(self.db.search_data('user',user_info)).drop(columns=['_id'])
                pass
        user_df[['gender','birthday', 'userId']] = user_df[['gender','birthday', 'userId']].astype(np.int)
        
        return user_df

    def get_newsfeed(self):

        match self.db_select:
            case "mongo":
                newsfeed_df = pd.DataFrame(self.db.search_data('newsfeed','')).drop(columns=['_id'])
                pass
        newsfeed_df[['status','likeCount','readCount','createdAt', 'userId','newsFeedId']] = newsfeed_df[['status','likeCount','readCount','createdAt', 'userId','newsFeedId']].astype(np.int)
        return newsfeed_df

    def get_user_like(self):

        match self.db_select:
            case "mongo":
                user_like_df = pd.DataFrame(self.db.search_data('user_like','')).drop(columns=['_id'])
                pass
        user_like_df[['userId','newsFeedId']] = user_like_df[['userId','newsFeedId']].astype(np.int)

        return user_like_df

    def get_data(self, data_name):

        match self.db_select:
            case "mongo":
                pass
            case "none":
                print(data_name)
                data_df = pd.read_pickle(f"{self.save_path}/{data_name}.pkl")

        return data_df

if __name__ == "__main__":

    data = Data(data_path='./data/recommended-data-sample 9_11.xlsx', db_select='none',config_path='./config/config.toml')
    data.load_data()
    user = data.get_data('user')
    newsfeed = data.get_data('newsfeed')
    user_like = data.get_data('user_like')
    topic_link = data.get_data('topic_link')
    user_create = data.get_data('user_create')
    user_newsfeed = data.get_data('user_newsfeed')
    user_user = data.get_data('user_user')
    newsfeed_newsfeed = data.get_data('newsfeed_newsfeed')
    
    pass
 
    