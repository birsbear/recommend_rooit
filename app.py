import json
from flask import Flask
from flask import request
from controll import *
app = Flask(__name__)
config_path = './config/config.toml'

def response_structure(newsFeeds_contain) -> str:
    """Response to the result of recommended newsFeed 

    #Args:
        newsFeeds_contain: 

    #Return:
        result: the json structure of total recommend result
    """
    
    response = json.dumps(newsFeeds_contain)
    return response

@app.route('/')
def load_data():
    rs = RecommendSystem(config_path)
    rs.data.load_data()
    return "Load data done"

@app.route("/recommend/meetPreference", methods = ['POST'])
def recommend_by_meetPreference():
    """Recommend user by user's meet gender preference
    
    """
    if request.method == "POST":
        
        rs = RecommendSystem(config_path)
        
        user_dict = request.form
        result = rs.recommend_by_meetPreference(user_dict)
        if isinstance(result, str):
            return result
        response = response_structure(result)
        return response
    else:
        
        return f"Request methods {request.method} worng"

@app.route("/recommend/newsFeedLike", methods = ['POST'])
def recommend_by_newsFeedLike():
    """Recommend user by user's recently liked newsFeed
    
    """
    if request.method == "POST":
        
        rs = RecommendSystem(config_path)
        
        user_dict = request.form
        result = rs.recommend_by_newsFeedLike(user_dict)
        if isinstance(result, str):
            return result
        response = response_structure(result)
        return response
    else:
        
        return f"Request methods {request.method} worng"

    
@app.route("/recommend/newsFeedHot", methods = ['POST'])
def recommend_by_newsFeedHot():
    """Recommend user by NewsFeed's Hot rank
    
    """
    if request.method == "POST":
        
        rs = RecommendSystem(config_path)
        
        user_dict = request.form
        result = rs.recommend_by_newsFeedHot(user_dict)
        if isinstance(result, str):
            return result
        response = response_structure(result)
        return response
    else:
        
        return f"Request methods {request.method} worng"

    
@app.route("/recommend/newsFeedTime", methods = ['POST'])
def recommend_by_newsFeedTime():
    """Recommend user by NewsFeed's post time
    
    """
    if request.method == "POST":
        
        rs = RecommendSystem(config_path)
        
        user_dict = request.form
        result = rs.recommend_by_newsFeedTime(user_dict)
        if isinstance(result, str):
            return result
        response = response_structure(result)
        return response
    else:
        
        return f"Request methods {request.method} worng"

    
@app.route("/recommend/newsFeedTopic", methods = ['POST'])
def recommend_by_newsFeedTopic():
    """Recommend user by NewsFeed's topic
    
    """
    if request.method == "POST":
        
        rs = RecommendSystem(config_path)
        
        user_dict = request.form
        result = rs.recommend_by_newsFeedTopic(user_dict)
        if isinstance(result, str):
            return result
        response = response_structure(result)
        return response
    else:
        
        return f"Request methods {request.method} worng"

    
@app.route("/recommend/age", methods = ['POST'])
def recommend_by_age():
    """Recommend user by user's age
    
    """
    if request.method == "POST":
        
        rs = RecommendSystem(config_path)
        
        user_dict = request.form
        result = rs.recommend_by_age(user_dict)
        if isinstance(result, str):
            return result
        response = response_structure(result)
        return response
    else:
        
        return f"Request methods {request.method} worng"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5002)