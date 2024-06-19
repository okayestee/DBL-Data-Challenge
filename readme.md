Manual 
Team Airplane Mode 
DBL 2023/2024 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
Contents 
Overview .................................................................................................................... 3 
Prerequisites: make sure you have the following libraries installed before running the 
codes 
.......................................................................................................................... 3 
Step 1: Set up development environment 
............................................................ 4 
Step 2: Unzip the data file 
.................................................................................... 4 
Step 3: Install MongoDB Community Server and MongoDB Shell 
....................... 5 
Step 4 Download the file 
...................................................................................... 5 
Step 5: Run Files ................................................................................................. 6 
Data cleaning ............................................................................................................. 7 
Business Idea 
............................................................................................................. 9 
Step 1: Check Names 
.......................................................................................... 9 
Step 2: Configure Dashboard .............................................................................. 9 
Step 3: Run File 
................................................................................................... 9 
Sentiment analysis ................................................................................................... 11 
Step 1: Check Names 
........................................................................................ 11 
Step 2: Configure Dashboard ............................................................................ 11 
Step 3: Run File 
................................................................................................. 12 
Conversation Mining................................................................................................. 13 
Sentiment Evolution ................................................................................................. 16 
Step 1: Check Names 
........................................................................................ 16 
Step 2: Configure Dashboard ............................................................................ 16 
Step 3: Run File 
................................................................................................. 16 
Appendices .............................................................................................................. 18 
If BERTopic has issues ......................................................................................... 18 
Creating random sample of Tweets ...................................................................... 18 
 
 
 
 
 
 
 
Overview 
This manual provides a comprehensive guide to setting up and running a script for 
mining conversation data from tweets. The script automates several tasks, including 
filtering replies, fetching starting tweets, identifying user conversation starters, 
constructing conversation trees, and storing the processed data into MongoDB 
collections for further analysis. Additionally, it offers various functions designed to 
process and clean tweet data stored in files, enabling efficient reading, removal of 
unnecessary information, and consolidation into a unified format. The manual also 
includes functionality to load cleaned tweet data into MongoDB, ensuring removal of 
duplicates and inconsistent data. 
Beyond data mining and cleaning, the business idea leverages this processed data 
to develop topic models using BERTopic, facilitating the extraction of meaningful 
conversation topics. Sentiment analysis is integrated to assess the emotional tone of 
tweets, generating sentiment scores (positive, neutral, negative) using VADER 
sentiment analysis tools. Furthermore, the manual guides users through analyzing 
sentiment evolution over time, plotting sentiment changes within conversations to 
identify shifts in user sentiment and engagement dynamics. 
 
Prerequisites: make sure you have the following libraries 
installed before running the codes 
• 
'pymongo' (install using 'pip install pymongo' in your terminal) 
• 
'treelib' (install using 'pip install treelib' in your terminal) 
• 
'tqdm' (install using 'pip install tqdm' in your terminal) 
• 
'matplotlib' (install using 'pip install matplotlib' in your terminal) 
• 
'nltk' (install using 'pip install nltk' in your terminal) 
• 
'click' (install using 'pip install click' in your terminal) 
• 
'bertopic' (install using 'pip install BERTopic' in your terminal) 
• 
'sklearn' (install using 'pip install scikit-learn' in your terminal) 
• 
‘Utility\_functions’ (not available on PYPI, it is obtained from your project’s 
repository or source) 
• 
‘rein’ ((not available on PYPI, it is obtained from your project’s repository or 
source) 
• 
‘bson’ (install using ‘pip install bson’ in your terminal) 
• 
‘gc’ (part of Python standard library, does not require installation) 
• 
‘json’ (part of Python standard library, does not require installation) 
 
 
 
Step 1: Set up development environment 
Make sure to download and install VSCode and download the correct version of 
python as well 
 
1. Visual Studio Code 
1. Download and install VSCode from the official website. 
2. Follow the installation instructions provided on the website. 
 
2. Python 3.11.9 
• 
Download and install Python version 3.11.9 from the Python Downloads page. 
• 
Follow the installation instructions provided on the website. 
 
Step 2: Unzip the data file 
Unzip the data file before running the code, you need to unzip a file named 
`data.zip`. This file should contain 567 JSON files, each containing tweets. 
 
1. Place the data.zip file in your working directory. 
 
2. Unzip the `data.zip` file into a folder called 'data' containing the JSON files. 
- On Windows: Right-click the `data.zip` file and select `Extract All...`, then follow 
the prompts. 
- On Mac: Double-click the `data.zip` file to unzip it. 
- On Linux: Use the command `unzip data.zip` in the terminal. 
 
Remark: 
The data folder containing all the data files of the different tweets should not be 
stored in a nested structure. 
 
 
 
 
Step 3: Install MongoDB Community Server and MongoDB Shell 
Make sure to install MongoDB and MongoDB Shell with the correct version 
 
 
1. Download and install MongoDB: 
• 
Visit the MongoDB download center and download [MongoDB Community 
Server Download] (MongoDB Community Server Download Page). 
• 
Choose your operating system and download the latest version '7.0.11' 
• 
Follow the installation instructions provided on the website. 
 
 
2. \*\*Download and install MongoDB Shell\*\*: 
• 
Visit the MongoDB Compass download page and download [MongoDB 
Compass Download (GUI)](MongoDB Compass Download Page). 
• 
Choose your operating system and download the installer for the latest 
version ‘2.2.9’. 
• 
Follow the installation instructions to complete the setup. 
 
3. \*\*Create a new database in your MongoDB\*\* 
• 
Open MongoDB and create a new database. 
• 
Create the database with the name "DBL" and collection name 
"cleaned\_data". Make sure that you name it exactly this otherwise the rest of 
the code becomes useless. 
 
 
Step 4 Download the file 
In order to run the business idea, you need to have the JSON file ‘topic\_share’ 
before you can run the file ‘Topic\_share\_dashboard.py’. The file should be placed in 
the same directory as the other folders (‘conversation\_mining’, ‘Business\_idea’, 
‘data\_cleaning’, ‘Sentiment’, etc.). 
You can download the file using this link: topic\_share.json 
 
 
 
 
Step 5: Run Files 
Find the ‘run and debug’ tab in the left side bar (left image) and click and green play 
button (right image) in the top left to execute the code. 
 
 
 
 
Run and debug tab icon 
Execute run and debug 
Data cleaning 
 
Task description 
We have thoroughly cleaned the tweet data by removing duplicates, filtering out 
inconsistencies, and ensuring all tweets are in English. This process involved 
reading tweets from files, removing unnecessary information, and consolidating the 
cleaned data into a single file for analysis. 
 
Go to VSCode, then follow the path: 'DBL-DATA-CHALLENGE' -> 
'data\_cleaning' -> 'Cleaning\_dashboard.py' -> ‘Cleaning\_dashboard\_step\_2’ 
and run the file(s): 
• 
Cleaning\_dashboard.py 
 
 
 
(with the correct path name) 
• 
Cleaning\_dashboard\_step\_2.py 
 
(with the correct path name) 
After having run the first dashboard, you will get a new JSON file called 
'cleaned\_data'. Afterwards, create a new database in your MongoDB called ‘DBL’ 
and create a new collection called 'cleaned\_data' and import the 'cleaned\_data'.json 
file into the new collection. You can do this by clicking "Import Data" after navigating 
to your collection and then clicking the cleaned\_data.json file. It should take 20 
minutes to complete. After it has finished importing, run the 
‘Cleaning\_dashboard\_step\_2’ file. 
 
If the dashboard does not work, then run the following files manually in that 
order: 
1. data\_cleans.py 
 
 
 
 
(in folder 'data\_cleaning') 
After having run data\_cleans.py, you will get a new .json file called 'cleaned\_data'. 
Afterwards, create a new database in your MongoDB and create a new collection 
called 'cleaned\_data' and import the 'cleaned\_data' .json file into the new collection. 
You can do this by clicking "Import Data" after navigating to your collection and then 
clicking the cleaned\_data.json file. It should take 20 minutes to complete. 
 
2. remove\_duplicates.py 
 
 
 
(in folder 'data\_cleaning') 
3. remove inconsistencies 
 
 
 
(in folder 'data\_cleaning') 
 
 
 
 
Functions: 
1. 'make\_tweet\_list' 
 
 
 
 
 
 
 
###Reads the data from the file located at the specified path and returns a list of 
tweets. 
2. 'file\_paths\_list' 
 
 
 
 
 
 
 
###Generates a list containing the paths to all files in the specified data folder. 
 
3. 'remove\_variables' 
 
 
 
 
 
 
 
###Cleans a given tweet by removing unnecessary variables. 
 
4. 'check\_language' 
 
 
 
 
 
 
 
###Checks whether a tweet is in English. 
 
5. 'check\_delete' 
 
 
 
 
 
 
 
 
###Checks whether the tweet is deleted or not. 
 
6. 'clean\_all\_files' 
 
 
 
 
 
 
 
###Cleans all data files in the specified folder and consolidates the cleaned 
tweets into a single file. 
 
7. 'make\_collection' 
 
 
 
 
 
 
 
###Creates a MongoDB collection and loads the cleaned tweet data into it. 
Export the 'cleaned\_data' file manually into your database called 'DBL'. 
 
8. 'remove\_duplicates' 
 
 
 
 
 
 
###Removes duplicate tweets from the MongoDB collection and consolidates 
unique tweets into a new collection called 'removed\_duplicates' in your database. 
 
9. 'remove\_inconsistencies' 
 
 
 
 
 
###Filters out inconsistent data from the MongoDB collection and stores 
consistent data in a new collection called 'no\_inconsistency' in your database. 
 
 
After you have finished running these files, you should have some collections 
that is called ‘cleaned\_data’, ‘removed\_duplicates’ and 'no\_inconsistency' in 
your database. This is all part of the data cleaning process that we have done. 
 
 
 
 
Business Idea 
 
Task description 
We are going to make a topic model which produces topics based on the data given. 
This data is cleaned (no duplicates and no inconsistencies). 
 
Step 1: Check Names 
Make sure your MongoDB Database is named 'DBL' and you have a collection 
named 'cleaned\_data'. The following code will return errors if the names don't match. 
 
Step 2: Configure Dashboard 
Go to VSCode, then follow the path: Go to VSCode, then follow the path: 'DBL-
DATA-CHALLENGE' -> 'Business\_idea' -> 'Topic\_share\_dashboard.py'. 
 
Change ‘DBL’ to whatever you named your MongoDB database if you didn’t follow 
the recommended naming convention in Prerequisites Step 3. Change ‘topics’ to 
whatever you named your topic analysis collection post the business idea stage if 
you didn’t follow the recommended naming convention in Prerequisites Step 3. 
 
Step 3: Run File 
Find the ‘run and debug’ tab in the left side bar (left image) and click and green play 
button (right image) in the top left to execute the code. 
Remark: 
This might take a very long time. If it takes more than a day to run, you can skip this 
part and just go to sentiment analysis. 
 
 
 
 
Run and debug tab icon 
Execute run and debug 
Functions: 
1. ‘make\_topic\_file’ 
### Generates a JSON file containing documents from MongoDB collection 
which is the collection topic\_analysis 
2. ‘get\_topic’ 
### Determines the topic label for a given text using a pre-trained BERTopic 
model 
3. ‘add\_topics’ 
### Updates a collection in a MongoDB database with assigned topics from 
topic\_share.json (from topic\_share) 
4. tweets\_without\_topic 
### Corrects documents in MongoDB collection (topics) that are missing a 
topic assignment 
 
By running this code, you will get a new collection in your MongoDB called 
‘topics’. This collection contains the same fields as the ‘no\_inconsistency’ 
collection, with an added field called ‘topic’ that contains the topic of the tweet. 
This approach not only facilitates understanding the prevalent topics within 
your data but also aids in deriving meaningful business insights from social 
media interactions. If the ‘Topic\_share\_dashboard.py’ did not run properly, you 
should continue with the collection called ‘no\_inconsistency’. 
 
 
 
 
Sentiment analysis 
 
Task description 
The code looks at the content of the tweet in terms of the text that was sent and 
calculates an overall sentiment score of the tweet based on each of the words it 
contains. Ultimately, a compound score is generated which is normalized for each 
and every tweet in the dataset. 
 
After having cleaned the data in the previous section, we can now allow VADER to 
individually create distribution of positive, neutral and negative scores and compute 
the compound score. 
 
After following these steps, a set in the following format will be outputted: 
{'neg': w, 'neu': x, 'pos': y, 'compound': z} where w, x, y, z are floating point numbers. 
 
Step 1: Check Names 
Make sure your MongoDB Database is named 'DBL' and you have a collection 
named ‘topics’, if the topic\_share\_dashboard.py worked. Otherwise you can use the 
‘no\_inconsistency’ collection for the rest of the manual 
 
Step 2: Configure Dashboard 
Go to VSCode, then follow the path: 'DBL-DATA-CHALLENGE' -> 'Sentiment' -> 
'Sentiment\_analysis\_dashboard.py'. In line 7 and 8, make the following edits if 
necessary. 
Line 7: db = client['DBL'] ## Use the DBL database 
Line 8: collection = db['topics'] ## Choose a collection of tweets (if the 
topic\_share\_dashboard.py worked), otherwise: 
Line 8: collection = db[‘no\_inconsistency’] ## Choose a collection of tweets 
 
Change ‘DBL’ to whatever you named your MongoDB database if you didn’t follow 
the recommended naming convention in Prerequisites Step 3. Change ‘topics’ to 
‘no\_inconsistency’ if the topic\_share\_dashboard.py did not work. 
 
 
Remark: 
If you don’t have the ‘topics’ collection because running the 
‘topic\_share\_dashboard.py’ file took too long to run, you can change line 7 to: 
Line 7: collection = db['no\_inconsistency']. 
This way you do have the sentiment scores of the tweets at least. 
 
Step 3: Run File 
Find the ‘run and debug’ tab in the left side bar (left image) and click and green play 
button (right image) in the top left to execute the code. 
 
 
 
 
 
 
 
Functions: 
1. update\_VADER 
### Creates and returns a VADER analyzer with custom words with custom 
values. 
2. analyze\_sentiment 
### Runs VADER on a piece of text and returns the sentiment scores. 
3. get\_full\_text 
### There are cases when long tweets are truncated and shortened with ellipses. 
This function will check if it has been shortened and attempt to obtain the full text 
and ignore the shortened one. 
4. add\_entire\_document 
### Inserts a document into a MongoDB collection. 
5. add\_sentiment\_variables 
###Creates a new collection and fills it with the documents from the old collection 
and adds the sentiment variables to all documents in the new collection. 
 
After running the codes, you should have a collection called 
‘sentiment\_included’ in your MongoDB database. This collection contains all 
the fields of the ‘no\_inconsistency’ collection with some added fields 
(‘compount\_sentiment’, ‘negativity’, ‘neutrality’, ‘positivity’, ‘truncated\_error). 
Run and debug tab icon 
Execute run and debug 
Conversation Mining 
 
Task description 
This part provides a comprehensive guide to setting up and running a script for 
mining conversation data from tweets. The script automates several tasks, including 
filtering replies, fetching starting tweets, identifying user conversation starters, 
constructing conversation trees, and storing the processed data into MongoDB 
collections for further analysis 
 
Go to VSCode, then follow the path: 'DBL-DATA-CHALLENGE' -> 
'conversation\_mining' -> 'dashboard convo.py' and run the file(s): 
• 
dashboard convo.py 
 
 
(in folder 'conversation\_mining') 
 
Remark: 
An error message will pop up once. The reason being is because the size of 
‘user\_trees.py’ is too big to store everything at once in a collection. You can skip the 
error with the steps below: 
 
Step 1: 
Run the dashboard with run and debug 
 
Step 2: 
Comment out the lines with files already run by highlighting it and clicking: ‘CTRL + /’ 
 
Step 3: 
Press the continue button on the horizontal run and debug toolbar 
 
Since the size is too big the dashboard also does not run the files after user.trees.py. 
After this file has finished running, you should comment out the lines that have 
already finished running. The pictures below represent it. 
 
 
 
Before: 
 
After: 
 
 
If the dashboard does not work, then run the following files manually in that 
order: 
1. filter\_replies.py 
 
 
 
(in folder 'conversation\_mining') 
2. collect\_starting\_conversation.py 
(in folder 'conversation\_mining') 
3. user\_starting\_convo.py 
 
 
(in folder 'conversation\_mining') 
4. airline\_starting\_convo.py 
 
(in folder 'conversation\_mining') 
5. user\_trees.py 
 
 
 
(in folder 'conversation\_mining') 
6. airline\_trees.py 
 
 
 
(in folder 'conversation\_mining') 
7. timeframe\_user\_trees.py 
 
(in folder 'conversation\_mining') 
8. timeframe\_airline\_trees.py 
 
(in folder 'conversation\_mining') 
9. tweet\_order\_user.py 
 
 
(in folder 'conversation\_mining') 
10. 
tweet\_order\_airline.py 
 
 
(in folder 'conversation\_mining') 
 
 
Functions: 
1. 'filter\_replies' 
 
 
 
 
 
###Extracts replies from the MongoDB collection of cleaned tweets and 
stores them in a new collection called 'replies'. 
2. 'collect\_starting\_conversations' 
 
 
 
 
###Extracts tweets that are not replies and stores them in a new collection 
called 'starting\_tweets'. 
3. 'user\_convo\_starters' 
 
 
 
 
 
 
###Identifies tweets from users (excluding airline accounts) that start 
conversations and stores them in a new collection called 
'user\_convo\_starters'. 
4. 'airline\_convo\_starters' 
 
 
 
 
 
 
### Identifies tweets from airline accounts that start conversations and stores 
them in a new collection called 'airline\_convo\_starters'. 
5. 'user\_trees' 
 
 
 
 
 
 
 
###Constructs conversation trees from the starting tweets of user 
conversations and stores them in a new collection called 'user\_trees' in your 
database. 
6. 'airline\_trees' 
 
 
 
 
 
 
 
###Constructs conversation trees from the starting tweets of airline 
conversations and stores them in a new collection called 'airline\_trees' in your 
database. 
7. 'timeframe\_user\_trees 
 
 
 
 
 
 
###Filters conversation trees from user tweets to include only tweets within a 
24-hour timeframe from their parent tweet and stores them in a new collection 
called 'timevertical\_trees\_user' in your database. 
8. 'timeframe\_airline\_trees' 
 
 
 
 
 
###Filters conversation trees to include only tweets within a 24-hour 
timeframe from their parent tweet and stores them in a new collection called 
'timevertical\_trees\_airline' in your database. 
9. 'tweet\_order\_user' 
 
 
 
 
 
 
###Validates conversation order in the conversation trees and stores valid 
user trees in a new collection called 'valid\_trees\_user' in your database. 
10. 
'tweet\_order\_airline' 
 
 
 
 
 
 
###Validates conversation order in the conversation trees and stores valid 
airline trees in a new collection called 'valid\_trees\_airline' in your database. 
11. 
‘merge\_valid\_trees’ 
 
###The function essentially consolidates documents from two separate 
collections (‘valid\_trees\_user’ and ‘valid\_trees\_airline’) into a single collection
 
 
 
 
 
 
 
 
 
After you have finished running all the codes, you should have many 
collections in your database. The three most important ones are 
'valid\_trees\_airline', 'valid\_trees\_user' and ‘valid\_trees\_merged’, these are the 
final conversations that match our definition. This concludes the conversation 
mining part. 
 
 
Sentiment Evolution 
 
Task description 
We can now collate all previous stages to finally understand the change in sentiment 
between tweets. The cleaned data is put through VADER in sentiment analysis and 
this function is later called in sentiment evolution after having structured the 
conversations. With the order clear, the sentiment scores are generated for the 
tweets and therefore it can be determined if preceding or succeeding tweets evolve 
in sentiment. Following the steps below will result in the final steps of this DBL. 
 
Step 1: Check Names 
Make sure your MongoDB Database is named 'DBL' and you have a collection 
named 'valid\_trees\_merged'. The following code will return errors if the names don't 
match. 
 
Step 2: Configure Dashboard 
Go to VSCode, then follow the path: 'DBL-DATA-CHALLENGE' -> 'Sentiment' -> 
'Sentiment\_evolution\_dashboard.py'. In line 6 and 7, make the following edits if 
necessary. 
Line 6: db = client['DBL'] ## Use the DBL database 
Line 7: collection = db[' valid\_trees\_merged'] ## Choose a collection of tweets 
 
Change ‘DBL’ to whatever you named your MongoDB database if you didn’t follow 
the recommended naming convention. Change ‘Convos’ to whatever you named 
your conversation mining collection if you didn’t follow the recommended naming 
convention. 
 
Step 3: Run File 
Find the ‘run and debug’ tab in the left side bar (left image) and click and green play 
button (right image) in the top left to execute the code. 
 
 
 
 
Run and debug tab icon 
Execute run and debug 
 
Functions: 
1. Get\_reply\_by\_index 
### Given a tree will get its a reply of a specified index. 
2. Get\_convo 
### Generates and returns a conversation as a list of tweets. 
3. extract\_compounds\_from\_convo\_vars 
###Returns the compound scores from all tweets in the conversation of a given 
tree. 
4. Get\_evolutions 
### Based on a list of compound scores, creates and returns a list containing 
both all of the evolutions and non-evolutions. 
5. count\_evolution\_types 
### Counts the amount of sentiment evolutions and non-evolutions and returns 
the counts as a dictionary. 
6. is\_airline\_userID 
### Checks whether a user ID belongs to an airline. 
7. get\_tree\_docs 
### Given a collection of trees, gets a list of all of the documents containing a 
topic if specified. If no topic is specified the list will contain all documents. 
8. get\_evolution\_stats 
### Calculates and returns the number of each evolution and non-evolution as a 
dictionary. 
9. plot\_evos 
### Plots the given evolutions on a chart. 
10. 
plot\_evo\_non\_evo 
### Plots a pie chart of the evolutions vs the non-evolutions. 
11. 
plot\_inc\_dec 
### Plots a chart comparing the increasing and decreasing evolutions. 
 
 
 
 
 
 
 
 
 
 
 
 
Appendices 
 
If BERTopic has issues 
• 
Press ‘Windows + r’ to open the Run dialog 
1. Type ‘regedit’ and press Enter to open the Registry Editor. 
2. Navigate to the following path: 
HKEY\_LOCAL\_MACHINE\SYSTEM\CurrentControlSet\Control\FileSystem 
3. Find the entry named ‘LongPathsEnabled’ 
4. Double-click on ‘LongPathsEnabled’ and set its value to ‘1’. 
5. Click OK and close the Registry Editor. 
 
To undo this: 
• 
Press ‘Windows + r’ to open the Run dialog 
6. Type ‘regedit’ and press Enter to open the Registry Editor. 
7. Navigate to the following path: 
HKEY\_LOCAL\_MACHINE\SYSTEM\CurrentControlSet\Control\FileSystem 
8. Find the entry named ‘LongPathsEnabled’ 
9. Double-click on ‘LongPathsEnabled’ and set its value to ‘0’. 
10. Click OK and close the Registry Editor. 
 
Creating random sample of Tweets 
Step 1 
Open the file "Random\_sample.py" after following the path: 
"DBL-Data-Challenge" >> "Sentiment" >> "Random\_sample.py" 
 
Step 2 
Scroll down to the end of the code and find the following line: 
sample = select\_random\_lines(500, "PATH TO CLEANED DATA FILE", 
count\_lines("PATH TO CLEANED DATA FILE")) 
 
1. Change the '500' integer to another integer if you want a different number of 
samples 
2. Write the path to where your cleaned data file is located. For example, If located 
here: C:\Users\USERNAME\OneDrive - TU Eindhoven\Documents\JBG030 - 
DBL Data Challenge\DBL-Data-Challenge\data, then replace the string with 
C:\\Users\\USERNAME\\OneDrive - TU Eindhoven\\Documents\\JBG030 - DBL 
Data Challenge\\DBL-Data-Challenge\\data 
 
Step 3: 
Run Random\_sample.py file by clicking the play button in the top right. It should 
output a random sample of tweets that is easy to read for a human interpreter in this 
format: Tweet X: {tweet text ...} 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
