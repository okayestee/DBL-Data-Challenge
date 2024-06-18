import runpy
# dashboard for filtering out the replies and roots
# and for creating and filtering the airline trees

def main():
    runpy.run_path("filter_replies.py")
    runpy.run_path("collect_starting_conversations.py")
    runpy.run_path("user_starting_convo.py")
    runpy.run_path("airline_starting_convo.py")
    runpy.run_path("user_trees.py")
    runpy.run_path("airline_trees.py")
    runpy.run_path("timeframe_user_trees.py") #make sure you reference to the tiairline_trees collection
    runpy.run_path("timeframe_airline_trees.py")
    runpy.run_path("tweet_order_user.py") #make sure you referene to the airline_trees collection
    runpy.run_path("tweet_order_airline.py")



if __name__ == "__main__":
    main()
