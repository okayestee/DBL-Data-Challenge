import runpy
# dashboard for filtering out the replies and roots
# and for creating and filtering the airline trees

def main():
    runpy.run_path("filter_replies.py")
    runpy.run_path("collect_starting_conversation.py")
    runpy.run_path("user_starting_convo.py")
    runpy.run_path("airline_starting_convo.py")
    runpy.run_path("airline_trees.py")
    runpy.run_path("timeframe.py")
    runpy.run_path("tweet_order")

if __name__ == "__main__":
    main()







