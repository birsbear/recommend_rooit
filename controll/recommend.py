import numpy as np
import pandas as pd
import tomllib
import datetime
from model.data_process import Data
from dataclasses import dataclass


def get_request_body(user_dict:dict) -> list[int|str]:
    """Get request body prarmeter

    #Args:
        user_dict: Request body prarmeter dict 

    #Return:
        birthday: User's birthday with unix timestamp
        meet_preference: User want to meet gender perference
        gender: User's gender 0 for female, 1 for male
        likes: User's recently liked newsFeed
        pageSize: Response newsFeed number
    """
    birthday = int(user_dict['birthday'])
    meet_preference = user_dict['meetPreference']
    gender = int(user_dict['gender'])
    if isinstance(user_dict['likes'][0], str):
        likes = [int(like_id) for like_id in user_dict['likes'].split(',')]
    if isinstance(user_dict['likes'][0], int):
        likes = user_dict['likes']
    pageSize = int(user_dict['pageSize'])
    
    return [birthday, meet_preference, gender, likes, pageSize]

@dataclass
class RecommendSystem:
    """Recommend System class

    #Property
    config_path: System config file path
    """
    config_path: str
    def __post_init__(self):
        with open(self.config_path, 'rb') as f:
            config = tomllib.load(f)
        data_path = config["args"]['data_path']
        db_select = config["args"]['db_select']
        self.data = Data(data_path=data_path, db_select=db_select, config_path=self.config_path)
    

    def recommend_by_meetPreference(self, user_dict:dict) -> dict:
        """Recommend newsFeed by user's meet preference

        #Agrs
        user_dict:User wait to recommend newsfeed info

        #Return
        result_newsfeed:Recommend newsfeed result. Number base on user_dict pageSize.
            Structure:
            {
                'id': Newsfeed's id
                'text':Newsfeed's text content
                'confindence':Newsfeed's confindence rate
            }
        """
        _, meet_preference, _, likes, pageSize = get_request_body(user_dict)
        likes = np.array(likes)
        user = self.data.get_user(meet_preference)
        newsfeed = self.data.get_newsfeed()
        user_like = self.data.get_user_like()

        match meet_preference:
            case "male":
                user_process = user[user['gender'] == 0]
            case "female":
                user_process = user[user['gender'] == 1]
            case "unlimited":
                user_process = user
            case _:
                return "error Meet Preference"

        user_process = user_process.reset_index(drop=True)
        user_process['userIndex'] = user_process.index.astype(str)
        user_like_process = user_process[['userIndex','userId']].merge(user_like.drop('userIndex', axis=1), how='left', on='userId')
        _, _, newsfeed_newsfeed_matrix = self.data.create_simlarty_matrix(user_process, newsfeed, user_like_process)
        top5_hot_newsfeed = newsfeed.sort_values(by=['readCount','likeCount'],ascending=False).iloc[:5]['newsFeedId']
        top5_hot_value = newsfeed_newsfeed_matrix.iloc[top5_hot_newsfeed.astype(int)-1].drop('newsfeed', axis=1).astype(np.float32)
        match len(likes)<5:
            case True:
                cold_start_rate = 1-len(likes)/5
            case False:
                cold_start_rate = 0
        user_like_value = newsfeed_newsfeed_matrix.iloc[likes-1].drop('newsfeed', axis=1).astype(np.float32)
        user_confidence = (user_like_value.sum(axis=0)*(1-cold_start_rate))/len(user_like_value) if len(user_like_value) != 0 else 0
        top5_hot_confidence = top5_hot_value.sum(axis=0)*cold_start_rate/5
        result_df = round((user_confidence+top5_hot_confidence)/0.2, 5).sort_values(ascending=False).reset_index()
        result_df.rename(columns={'index':'newsFeedId', 0:'confidence'}, inplace=True)
        result_df['newsFeedId'] = result_df.apply(lambda x : x['newsFeedId'].replace('newsfeed_', ''), axis=1).astype(int)
        result_df = result_df.merge(newsfeed[['newsFeedId','text']], how='left',on='newsFeedId')
        result_df = result_df.rename(columns={'newsFeedId':'id'})
        result_df['confidence'] = (result_df['confidence']*5/result_df['confidence'].max())
        result_df = result_df.sort_values(by=['confidence'], ascending=False).iloc[:pageSize].reset_index(drop=True)
        result_newsfeed = {ind:i.to_dict() for ind, i in result_df[['id','text','confidence']].iterrows()}

        return result_newsfeed

    def recommend_by_newsFeedLike(self, user_dict:dict) -> dict:
        """Recommend newsFeed by user's like newsfeed

        #Agrs
        user_dict:User wait to recommend newsfeed info

        #Return
        result_newsfeed:Recommend newsfeed result. Number base on user_dict pageSize.
            Structure:
            {
                'id': Newsfeed's id
                'text':Newsfeed's text content
                'confindence':Newsfeed's confindence rate
            }
        """
        _, _, _, likes, pageSize = get_request_body(user_dict)
        likes = np.array(likes)
        
        user_process = self.data.get_user()
        newsfeed = self.data.get_newsfeed()
        user_like_process = self.data.get_user_like()
        
        _, _, newsfeed_newsfeed_matrix = self.data.create_simlarty_matrix(user_process, newsfeed, user_like_process)
        top5_hot_newsfeed = newsfeed.sort_values(by=['readCount','likeCount'],ascending=False).iloc[:5]['newsFeedId']
        top5_hot_value = newsfeed_newsfeed_matrix.iloc[top5_hot_newsfeed.astype(int)-1].drop('newsfeed', axis=1).astype(np.float32)
        match len(likes)<5:
            case True:
                cold_start_rate = 1-len(likes)/5
            case False:
                cold_start_rate = 0
        newsFeed_like_value = newsfeed_newsfeed_matrix.iloc[likes-1].drop('newsfeed', axis=1).astype(np.float32)
        newsFeed_confidence = (newsFeed_like_value.sum(axis=0)*(1-cold_start_rate))/len(newsFeed_like_value) if len(newsFeed_like_value) != 0 else 0
        top5_hot_confidence = top5_hot_value.sum(axis=0)*cold_start_rate/5
        result_df = round((newsFeed_confidence+top5_hot_confidence)/0.2, 5).sort_values(ascending=False).reset_index()
        result_df.rename(columns={'index':'newsFeedId', 0:'confidence'}, inplace=True)
        result_df['newsFeedId'] = result_df.apply(lambda x : x['newsFeedId'].replace('newsfeed_', ''), axis=1).astype(int)
        result_df = result_df.merge(newsfeed[['newsFeedId','text']], how='left',on='newsFeedId')
        result_df = result_df.rename(columns={'newsFeedId':'id'})
        result_df['confidence'] = (result_df['confidence']*5/result_df['confidence'].max())
        result_df = result_df.sort_values(by=['confidence'], ascending=False).iloc[:pageSize].reset_index(drop=True)
        result_newsfeed = {ind:i.to_dict() for ind, i in result_df[['id','text','confidence']].iterrows()}

        return result_newsfeed
    
    def recommend_by_newsFeedHot(self, user_dict:dict) -> dict:
        """Recommend newsFeed by Hot rate with newsfeed

        #Agrs
        user_dict:User wait to recommend newsfeed info

        #Return
        result_newsfeed:Recommend newsfeed result. Number base on user_dict pageSize.
            Structure:
            {
                'id': Newsfeed's id
                'text':Newsfeed's text content
                'confindence':Newsfeed's confindence rate
            }
        """
        _, _, _, likes, pageSize = get_request_body(user_dict)
        likes = np.array(likes)
        
        user_process = self.data.get_user()
        newsfeed = self.data.get_newsfeed()
        user_like_process = self.data.get_user_like()
        max_like, max_read = newsfeed[['likeCount', 'readCount']].max()
        _, _, newsfeed_newsfeed_matrix = self.data.create_simlarty_matrix(user_process, newsfeed, user_like_process)
        top5_hot_newsfeed = newsfeed.sort_values(by=['readCount','likeCount'],ascending=False).iloc[:5]['newsFeedId']
        top5_hot_value = newsfeed_newsfeed_matrix.iloc[top5_hot_newsfeed.astype(int)-1].drop('newsfeed', axis=1).astype(np.float32)
        match len(likes)<5:
            case True:
                cold_start_rate = 1-len(likes)/5
            case False:
                cold_start_rate = 0
        newsFeed_like_value = newsfeed_newsfeed_matrix.iloc[likes-1].drop('newsfeed', axis=1).astype(np.float32)
        newsFeed_confidence = (newsFeed_like_value.sum(axis=0)*(1-cold_start_rate))/len(newsFeed_like_value) if len(newsFeed_like_value) != 0 else 0
        top5_hot_confidence = top5_hot_value.sum(axis=0)*cold_start_rate/5
        
        result_df = round((newsFeed_confidence+top5_hot_confidence)/0.2, 5).sort_values(ascending=False).reset_index()
        result_df.rename(columns={'index':'newsFeedId', 0:'confidence'}, inplace=True)
        result_df['newsFeedId'] = result_df.apply(lambda x : x['newsFeedId'].replace('newsfeed_', ''), axis=1).astype(int)
        result_df = result_df.merge(newsfeed[['newsFeedId','text','readCount','likeCount']], how='left',on='newsFeedId')
        result_df = result_df.rename(columns={'newsFeedId':'id'})
        result_df['hotConfidence'] = result_df.apply(lambda x : x['confidence']*x['likeCount']/max_like*x['readCount']/max_read, axis=1)
        result_df['confidence'] = (result_df['hotConfidence']*5/result_df['hotConfidence'].max())
        result_df = result_df.sort_values(by=['confidence'], ascending=False).iloc[:pageSize].reset_index(drop=True)
        result_newsfeed = {ind:i.to_dict() for ind, i in result_df[['id','text','confidence']].iterrows()}

        return result_newsfeed
    
    def recommend_by_newsFeedTime(self, user_dict:dict) -> dict:
        """Recommend newsFeed by create time

        #Agrs
        user_dict:User wait to recommend newsfeed info

        #Return
        result_newsfeed:Recommend newsfeed result. Number base on user_dict pageSize.
            Structure:
            {
                'id': Newsfeed's id
                'text':Newsfeed's text content
                'confindence':Newsfeed's confindence rate
            }
        """
        _, _, _, likes, pageSize = get_request_body(user_dict)
        likes = np.array(likes)
        
        user_process = self.data.get_user()
        newsfeed = self.data.get_newsfeed()
        user_like_process = self.data.get_user_like()
        _, _, newsfeed_newsfeed_matrix = self.data.create_simlarty_matrix(user_process, newsfeed, user_like_process)
        top5_hot_newsfeed = newsfeed.sort_values(by=['readCount','likeCount'],ascending=False).iloc[:5]['newsFeedId']
        top5_hot_value = newsfeed_newsfeed_matrix.iloc[top5_hot_newsfeed.astype(int)-1].drop('newsfeed', axis=1).astype(np.float32)
        match len(likes)<5:
            case True:
                cold_start_rate = 1-len(likes)/5
            case False:
                cold_start_rate = 0
        newsFeed_like_value = newsfeed_newsfeed_matrix.iloc[likes-1].drop('newsfeed', axis=1).astype(np.float32)
        newsFeed_confidence = (newsFeed_like_value.sum(axis=0)*(1-cold_start_rate))/len(newsFeed_like_value) if len(newsFeed_like_value) != 0 else 0
        top5_hot_confidence = top5_hot_value.sum(axis=0)*cold_start_rate/5
        
        result_df = round((newsFeed_confidence+top5_hot_confidence)/0.2, 5).sort_values(ascending=False).reset_index()
        result_df.rename(columns={'index':'newsFeedId', 0:'confidence'}, inplace=True)
        result_df['newsFeedId'] = result_df.apply(lambda x : x['newsFeedId'].replace('newsfeed_', ''), axis=1).astype(int)
        result_df = result_df.merge(newsfeed[['newsFeedId','text','createdAt']], how='left',on='newsFeedId')
        result_df = result_df.rename(columns={'newsFeedId':'id'})
        result_df['timeConfidence'] = result_df.apply(lambda x : int((datetime.datetime.now().timestamp()-x['createdAt'])/10000), axis=1)
        result_df['timeConfidence'] = result_df['timeConfidence']-result_df['timeConfidence'].min()
        result_df['timeConfidence'] = result_df.apply(lambda x : 1-(x['timeConfidence']/1000 if x['timeConfidence']< 1000 else 0.001), axis=1)
        result_df['timeConfidence'] = result_df.apply(lambda x : x['confidence']*x['timeConfidence'], axis=1)
        result_df['confidence'] = (result_df['timeConfidence']*5/result_df['timeConfidence'].max())
        result_df = result_df.sort_values(by=['confidence'], ascending=False).iloc[:pageSize].reset_index(drop=True)
        result_newsfeed = {ind:i.to_dict() for ind, i in result_df[['id','text','confidence']].iterrows()}

        return result_newsfeed
    
    def recommend_by_newsFeedTopic(self, user_dict:dict) -> dict:
        """Recommend newsFeed by newsfeed's Topic

        #Agrs
        user_dict:User wait to recommend newsfeed info

        #Return
        result_newsfeed:Recommend newsfeed result. Number base on user_dict pageSize.
            Structure:
            {
                'id': Newsfeed's id
                'text':Newsfeed's text content
                'confindence':Newsfeed's confindence rate
            }
        """
        _, _, _, likes, pageSize = get_request_body(user_dict)
        likes = np.array(likes)
        
        user_process = self.data.get_user()
        newsfeed = self.data.get_newsfeed()
        user_like_process = self.data.get_user_like()
        
        _, _, newsfeed_newsfeed_matrix = self.data.create_simlarty_matrix(user_process, newsfeed, user_like_process)
        top5_hot_newsfeed = newsfeed.sort_values(by=['readCount','likeCount'],ascending=False).iloc[:5]['newsFeedId']
        top5_hot_value = newsfeed_newsfeed_matrix.iloc[top5_hot_newsfeed.astype(int)-1].drop('newsfeed', axis=1).astype(np.float32)
        match len(likes)<5:
            case True:
                cold_start_rate = 1-len(likes)/5
            case False:
                cold_start_rate = 0
        user_like_topic_count = newsfeed[newsfeed['newsFeedId'].isin(likes)]['topic'].value_counts()
        topic_value = user_like_topic_count/len(likes)
        newsFeed_like_value = newsfeed_newsfeed_matrix.iloc[likes-1].drop('newsfeed', axis=1).astype(np.float32)
        newsFeed_confidence = (newsFeed_like_value.sum(axis=0)*(1-cold_start_rate))/len(newsFeed_like_value) if len(newsFeed_like_value) != 0 else 0
        top5_hot_confidence = top5_hot_value.sum(axis=0)*cold_start_rate/5
        result_df = round((newsFeed_confidence+top5_hot_confidence)/0.2, 5).sort_values(ascending=False).reset_index()
        result_df.rename(columns={'index':'newsFeedId', 0:'confidence'}, inplace=True)
        result_df['newsFeedId'] = result_df.apply(lambda x : x['newsFeedId'].replace('newsfeed_', ''), axis=1).astype(int)
        result_df = result_df.merge(newsfeed[['newsFeedId','text','topic']], how='left',on='newsFeedId')
        result_df = result_df.rename(columns={'newsFeedId':'id'})
        result_df['topic_confidence'] = result_df.apply(lambda x : 1+topic_value.get(x['topic'], 0), axis=1)
        result_df['topic_confidence'] = result_df.apply(lambda x : x['confidence']*x['topic_confidence'], axis=1)
        result_df['confidence'] = (result_df['topic_confidence']*5/result_df['topic_confidence'].max())
        result_df = result_df.sort_values(by=['confidence'], ascending=False).iloc[:pageSize].reset_index(drop=True)
        result_newsfeed = {ind:i.to_dict() for ind, i in result_df[['id','text','confidence']].iterrows()}

        return result_newsfeed
    
    def recommend_by_age(self, user_dict:dict) -> dict:
        """Recommend newsFeed by user's age

        #Agrs
        user_dict:User wait to recommend newsfeed info

        #Return
        result_newsfeed:Recommend newsfeed result. Number base on user_dict pageSize.
            Structure:
            {
                'id': Newsfeed's id
                'text':Newsfeed's text content
                'confindence':Newsfeed's confindence rate
            }
        """
        birthday, _, _, likes, pageSize = get_request_body(user_dict)
        likes = np.array(likes)
        
        user_process = self.data.get_user()
        newsfeed = self.data.get_newsfeed()
        user_like_process = self.data.get_user_like()
        _, _, newsfeed_newsfeed_matrix = self.data.create_simlarty_matrix(user_process, newsfeed, user_like_process)
        top5_hot_newsfeed = newsfeed.sort_values(by=['readCount','likeCount'],ascending=False).iloc[:5]['newsFeedId']
        top5_hot_value = newsfeed_newsfeed_matrix.iloc[top5_hot_newsfeed.astype(int)-1].drop('newsfeed', axis=1).astype(np.float32)
        match len(likes)<5:
            case True:
                cold_start_rate = 1-len(likes)/5
            case False:
                cold_start_rate = 0
        newsFeed_like_value = newsfeed_newsfeed_matrix.iloc[likes-1].drop('newsfeed', axis=1).astype(np.float32)
        newsFeed_confidence = (newsFeed_like_value.sum(axis=0)*(1-cold_start_rate))/len(newsFeed_like_value) if len(newsFeed_like_value) != 0 else 0
        top5_hot_confidence = top5_hot_value.sum(axis=0)*cold_start_rate/5
        
        result_df = round((newsFeed_confidence+top5_hot_confidence)/0.2, 5).sort_values(ascending=False).reset_index()
        result_df.rename(columns={'index':'newsFeedId', 0:'confidence'}, inplace=True)
        result_df['newsFeedId'] = result_df.apply(lambda x : x['newsFeedId'].replace('newsfeed_', ''), axis=1).astype(int)
        result_df = result_df.merge(newsfeed[['newsFeedId','text','createdAt','userId']], how='left',on='newsFeedId')
        result_df = result_df.merge(user_process[['userId','birthday']], how='left',on='userId')
        result_df = result_df.rename(columns={'newsFeedId':'id'})
        result_df['birthdayConfidence'] = result_df.apply(lambda x : float((100-datetime.timedelta(seconds=(birthday-x['birthday'])).days/365)/100), axis=1)
        result_df['birthdayConfidence'] = result_df.apply(lambda x : x['confidence']*x['birthdayConfidence'], axis=1)
        result_df['confidence'] = (result_df['birthdayConfidence']*5/result_df['birthdayConfidence'].max())
        result_df = result_df.sort_values(by=['confidence'], ascending=False).iloc[:pageSize].reset_index(drop=True)
        result_newsfeed = {ind:i.to_dict() for ind, i in result_df[['id','text','confidence']].iterrows()}

        return result_newsfeed

