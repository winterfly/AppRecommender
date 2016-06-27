from pymongo import MongoClient
import operator
import math

class DataBase(object):
    
    @classmethod
    def retrieve_app_info(cls):
        list = []
        for app_post in cls.db.app_info.find():
            list.append(app_post['app_id'])
        return list
    
    @classmethod
    def retrieve_user_download_history(cls):
        list = []
        for history in cls.db.user_download_history.find():
            list.append(history['download_history'])
        return list

    @classmethod
    def init(cls, client):
        cls.db = client.appstore
        cls.app_info = cls.retrieve_app_info()
        cls.user_download_history = cls.retrieve_user_download_history()

    @classmethod
    def fetch_top_5(cls):
        for app in cls.app_info:
            score = {}
            for apps in cls.user_download_history:
                if app in apps:
                    continue
                similarity = float(1) / math.sqrt(len(apps))
                for other_app in apps:
                    if app is other_app:
                        continue
                    if score.has_key(other_app):
                        score[other_app] = score[other_app] + similarity
                    else:
                        score[other_app] = similarity

            if not score:
                continue

#        score = sorted(score.items(), key=operator.itemgetter(0))
            sorted_tups = sorted(score.items(), key=operator.itemgetter(1), reverse=True)
            top_5_app = [sorted_tups[0][0], sorted_tups[1][0], sorted_tups[2][0], sorted_tups[3][0], sorted_tups[4][0]]
            cls.db.app_info.update_one({'app_id' : app}, {'$set': {'top_5_app': top_5_app}}, True)

def main():
    try:
        # get MongoDB client
        client = MongoClient()
        # save date in DataBase
        DataBase.init(client)
        # get top 5 app for each app
        DataBase.fetch_top_5()

    except Exception as e:
        print("Exception detected:")
        print(e)
    finally:
        # clean up work
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    main()

