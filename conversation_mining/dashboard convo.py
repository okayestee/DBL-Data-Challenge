import subprocess
import sys

def run_script(script_path):
    try:
        print(f"Running {script_path}...")
        result = subprocess.run([sys.executable, script_path], check=True)
        print(f"Completed {script_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running {script_path}: {e}")

def main():
    scripts = [
        "conversation_mining\\filter_replies.py",
        "conversation_mining\\collect_starting_conversations.py",
        "conversation_mining\\user_starting_convo.py",
        "conversation_mining\\airline_starting_convo.py",
        "conversation_mining\\user_trees.py",
        "conversation_mining\\airline_trees.py",
        "conversation_mining\\timeframe_user_trees.py",  # Make sure you reference to the airline_trees collection
        "conversation_mining\\timeframe_airline_trees.py",
        "conversation_mining\\tweet_order_user.py",      # Make sure you reference to the airline_trees collection
        "conversation_mining\\tweet_order_airline.py",
        "conversation_mining\\merge_valid_trees.py"
    ]
    
    for script in scripts:
        run_script(script)

# Execute the main function
if __name__ == "__main__":
    main()
