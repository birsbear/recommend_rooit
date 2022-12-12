# Recommend Newsfeed System

1. 使用command移到資料夾後部屬docker

    ``docker-compose up``


2. 使用 GET 訪問首頁 port為5002 (http://localhost:5002/) 載入檢視者資料進資料庫

    ``GET http://localhost:5002/``

    載入完成後會返回 "Load data done"

3. 使用 POST 輸入欲推薦檢視者資訊，訪問連結對應六個推薦方向
   1. 檢視者性向           
        ``POST http://localhost:5002/recommend/meetPreference``
   2. 最近喜歡的NewsFeed    
        ``POST http://localhost:5002/recommend/newsFeedLike``
   3. NewsFeed 熱門度      
        ``POST http://localhost:5002/recommend/newsFeedHot``
   4. NewsFeed 發文時間    
        ``POST http://localhost:5002/recommend/newsFeedTime``
   5. NewsFeed Topic      
        ``POST http://localhost:5002/recommend/newsFeedTopic``
   6. 檢視者對應發文者年紀  
        ``POST http://localhost:5002/recommend/age``

    Request body

        {
            birthday: number //unix timestamp
            meet_preference:  'unlimited'|'female'|'male' //str
            gender: 0|1 // female:0, male:1
            likes: list[number] //最近喜歡的Nnewsfeed ID 可能為空 list內為int
            pageSize: number  //預計回傳的數量
        }
    
    Response

        {
            NewsFeeds:{
                id: number //NewsFeed ID
                text: string //NewsFeed 內文
                confidence: number //NewsFeed 推薦度
            }
        }
