import runpy
import pymongo
# dashboard for filtering out the replies and roots
# and for creating and filtering the airline trees

client = pymongo.MongoClient("mongodb://localhost:27017/") ## Connect to MongoDB
db = client['DBL'] ## Use the DBL database

def main():
    runpy.run_path("conversation_mining\\filter_replies.py")
    runpy.run_path("conversation_mining\\collect_starting_conversations.py")
    runpy.run_path("conversation_mining\\user_starting_convo.py")
    runpy.run_path("conversation_mining\\airline_starting_convo.py")
    runpy.run_path("conversation_mining\\user_trees.py")
    runpy.run_path("conversation_mining\\airline_trees.py")
    runpy.run_path("conversation_mining\\timeframe_user_trees.py")
    runpy.run_path("conversation_mining\\timeframe_airline_trees.py")
    runpy.run_path("conversation_mining\\tweet_order_user.py") 
    runpy.run_path("conversation_mining\\tweet_order_airline.py")



if __name__ == "__main__":
    main()







