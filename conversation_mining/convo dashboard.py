import runpy
# dashboard for filtering out the replies and roots
# and for creating and filtering the airline trees

def main():
    runpy.run_path("filter_replies.py")
    runpy.run_path("collect_starting_conversation.py")
    runpy.run_path("user_starting_convo.py")
    runpy.run_path("airline_starting_convo.py")
    runpy.run_path("airline_trees.py")
    runpy.run_path("timeframe.py") #make sure you reference to the airline_trees collection
    runpy.run_path("tweet_order.py") #make sure you referene to the airline_trees collection

'''
after you ran the code above, uncomment the code below and
make sure you now reference to the user_trees collection
'''
#def main():
    #runpy.run_path("user_trees.py")
   # runpy.run_path("timeframe.py")
    #runpy.run_path("tweet_order.py")



if __name__ == "__main__":
    main()







