import pandas as pd
from dateutil.parser import parse

def is_date(string, fuzzy=False):
    """
    Return a value indicating whether the string may be understood as a date. .
    :param string: str, string to check for date
    :param fuzzy: bool, if True, ignore any unidentified tokens in the string. 
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False
# # downloading files from google drive
# import gdown
# url = 'https://drive.google.com/uc?id=1qZs7zZQ90w9jtZF4ptMHmgrW0KbqLEU5'
# output = 'WhatsApp Chat with Young Data Professionals.txt'
# gdown.download(url, output, quiet=False)

# downloading files in folders from google drive
import gdown
url = 'https://drive.google.com/drive/folders/15BGSF7Xvv4nbd9rVI9ooJSToaAWLQAx5?usp=sharing'
files = gdown.download_folder(url)


def convert_whatsapp_messages_to_list(f):
    """
    To convert WhatsApp messages from a file to a list,
    
    :param f: FileBuffer, FileBuffer with the opened file in read mode
    :return lines: List of WhatsApp Messages in 2 dimensional list
    """
    lines = []
    for line in f.readlines():
        line_list = line.replace("\n","").split(",")
        if is_date(line_list[0]):
            lines.append([line_list[0],("".join(line_list[1:]))])
        else:
            lines[-1][-1] = lines[-1][-1] +' '+ line.replace("\n","")
    return lines


# Open the file and then call the function to convet the messages to list
for file in files:
    f = open(file, encoding = "utf8")
    messages = convert_whatsapp_messages_to_list(f)

# change the format to dataframe
messages_df = pd.DataFrame(messages,columns=['date','message'])

# Extraction of the DataFrame's Time and Message 
time_msg = messages_df["message"].str.split("-", n = 1, expand = True)
messages_df["time"] = time_msg[0]
messages_df["message"] = time_msg[1]

#Extraction of the User Who Posted the Message posted and The Actual Message
user_msg = messages_df["message"].str.split(":", n = 1, expand = True)
messages_df["author"] = user_msg[0]
messages_df["message"] = user_msg[1]

messages_df['id'] = range(1, 1+len(messages_df))

# Create a column for timestamp
messages_df["timestamp"] = (messages_df["date"] +' ' + messages_df["time"])
messages_df = messages_df[["id","date","time","timestamp","author","message"]]

import re

def extract_url(message):
    """
    Regex to Extract URLs from Messages (Stackoverflow Help :smiley:)
    """
    url = r"(http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)"
    urls_extract = messages_df.message.str.extractall(url)
    url_id = []

    for i in range(0,len(urls_extract)):
    
        add_list = urls_extract.index[[i]][0][0] + 1
    
        url_id.append(add_list)
    urls_extract['id'] = url_id

    urls_extract.head()
    return urls_extract

links = extract_url(messages_df["message"])

# merging the daframe containinng the imessages and those containing links
data = pd.merge(messages_df, links, on = "id", how = "inner")

data.columns = ['id', 'date', 'time', 'timestamp','author', 'message', 'url']
data.to_csv("data/results.csv")

# search for words and then put those links inside a folder
def search(regex: str, df, case=False):
    """Search all the text columns of `df`, return rows with any matches."""
    textlikes = df.select_dtypes(include=[object, "string"])
    return df[
        textlikes.apply(
            lambda column: column.str.contains(regex, regex=True, case=case, na=False)
        ).any(axis=1)
    ]
# save the message and link to a folder
data_analytics = search("data analytics", data)
data_analytics = data_analytics[['url', 'timestamp','message']]
data_analytics = data_analytics.reset_index()
data_analytics.drop(columns = "index", inplace = True)
data_analytics.head()
data_analytics.to_csv("data/Data analytics.csv", index=False)

data_engineering = search("data engineering", data)
data_engineering = data_engineering[['url','timestamp', 'message']]
data_engineering = data_engineering.reset_index()
data_engineering.drop(columns = "index", inplace = True)
data_engineering.to_csv("data/Data engineering.csv")

machine_learning = search("machine learning", data)
machine_learning = machine_learning[['url','timestamp','message']]
machine_learning = machine_learning.reset_index()
machine_learning.drop(columns = "index", inplace = True)
machine_learning.to_csv("data/Machine learning.csv")

finance = search("onyeka", data)
finance = finance[['url', 'timestamp', 'message']]
finance = finance.reset_index()
finance.drop(columns = "index", inplace = True)
finance.to_csv("data/Finance.csv")

data_analysis = search("data analysis", data)
data_analysis = data_analysis[['url', 'timestamp', 'message']]
data_analysis = data_analysis.reset_index()
data_analysis.drop(columns = "index", inplace = True)
data_analysis.to_csv("data/Data analysis.csv")

power_bi = search("powerbi", data)
power_bi = power_bi[['url','timestamp', 'message']]
power_bi = power_bi.reset_index()
power_bi.drop(columns = "index", inplace = True)
power_bi.to_csv("data/PowerBi.csv")

MS_excel = search("excel", data)
MS_excel = MS_excel[['url','timestamp','message']]
MS_excel = MS_excel.reset_index()
MS_excel.drop(columns = "index", inplace = True)
MS_excel.to_csv("data/MS excel.csv")

SQL_file = search("sql", data)
SQL_file = SQL_file[['url','timestamp', 'message']]
SQL_file = SQL_file.reset_index()
SQL_file.drop(columns = "index", inplace = True)
SQL_file.to_csv("data/SQL file.csv")
