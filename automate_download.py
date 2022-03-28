
## Load Libraries
import pandas as pd
from dateutil.parser import parse
import gdown
import re

## Custom Functions

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


def convert_chat_list_to_df(chat_list):
    
    # change the format to dataframe
    messages_df = pd.DataFrame(chat_list,columns=['date','message'])

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
    messages_df['datetime'] = pd.to_datetime(messages_df.date + messages_df.time, errors='coerce', dayfirst=True)
    messages_df = messages_df[["id","datetime","author","message"]]

    return messages_df

def filter_data(df, last_record):

    new_records = df[df.datetime > last_record[0]]

    latest_date = max(new_records.datetime)
            
    with open('data/latest_date.txt', 'w') as f:
        f.write(str(latest_date))

    print("New Date saved successfully")

    return new_records


def extract_url(df):

    """
    Regex to Extract URLs from Messages (Stackoverflow Help :smiley:)
    """
    url = r"(http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)"
    urls_extract = df.message.str.extractall(url)

    if len(urls_extract) == 0:
        print("No link in this batch")

    else:

        url_id = []

        for i in range(0,len(urls_extract)):
        
            add_list = urls_extract.index[[i]][0][0] + 1

            url_id.append(add_list)

        urls_extract['id'] = url_id

        links_tbl = pd.merge(df, urls_extract, on = "id", how = "inner")

        links_tbl.columns = ['id', 'datetime', 'author', 'message', 'url']

    return links_tbl

def search(regex: str, df, case=False):
                """Search all the text columns of `df`, return rows with any matches."""
                textlikes = df.select_dtypes(include=[object, "string"])
                return df[
                    textlikes.apply(
                        lambda column: column.str.contains(regex, regex=True, case=case, na=False)
                    ).any(axis=1)
                ]

def save_types(df, key_word, type):
    filtered_data = search(key_word, df).drop_duplicates()

    if(len(filtered_data) == 0):
        print(f"No links for {key_word} ")

    else:
        filtered_data = filtered_data[['datetime','url','message']]
        filtered_data = filtered_data.reset_index()
        filtered_data.drop(columns = "index", inplace = True)

        start = str(min(filtered_data.datetime)).split(" ")[0]
                    
        end = str(max(filtered_data.datetime)).split(" ")[0]

        filtered_data.to_csv(f"data/Extracted Links/{type}/{start}to{end}.csv", index=False)

        print(f"File saved to data/Extracted Links/{type}/{start}to{end}.csv")

def export_link(df):
    
    start = str(min(df.datetime)).split(" ")[0]
                
    end = str(max(df.datetime)).split(" ")[0]
    
    df.to_csv(f"data/Extracted Links/Main/{start}to{end}.csv", index=False)

    print(f"Saved sucessfully to data/Extracted Links/Main/{start}to{end}.csv")

## 

## Load Whatsapp chat saved in google drive
try:
    url = 'https://drive.google.com/drive/folders/1YRKcXfLnFtQBTq68O4QiaUwtN7dz-Y8p?usp=sharing'
    
    files = gdown.download_folder(url)

except:
    print("Error in loading data")

else:

    try: 
        for file in files:
            f = open(file, encoding = "utf8")
            messages = convert_whatsapp_messages_to_list(f)

        last_date = open("data/latest_date.txt", encoding = "utf8")
        last_record = last_date.readlines()

    except:
        print("Error in reading records")
    else:
        print("Reading whatsapp data and last date")
        
        message_df = convert_chat_list_to_df(messages)

        if max(message_df.datetime) <= pd.to_datetime(last_record[0], yearfirst=True):

            print("Records are upto date")

        else:

            filtered_df = filter_data(message_df, last_record)

            links = extract_url(filtered_df)

            export_link(links)

            groups = ["data analytics", "data engineering", "machine learning",
                        "onyeka", "data analysis", "powerbi|power bi",
                        "excel", "sql"]

            save_to = ["data analytics", "data engineering", "machine learning",
                        "finance", "data analysis", "powerbi",
                        "ms excel", "sql"]

            if (len(groups) == len(save_to)):

                for i in range(0,len(groups)):
                    save_types(links, groups[i], save_to[i])
            else:
                print("groups is not equal to save_to")

