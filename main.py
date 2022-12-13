from keybert import KeyBERT
from collections import namedtuple
import json
from langdetect import detect_langs
from googleapiclient.discovery import build
import re
from rake_nltk import Rake
import sys
import os
import pyspark
from pyspark.sql.functions import count
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType, StringType
from pyspark.sql.functions import col


# Api_keys for connection to Youtube data API v3.

# api_key=""
# api_key=""
# api_key=""

# Method for getting list of youtube videos IDS and saving them to file.
# ID is 11 characters long string after https://www.youtube.com/watch?v=
def videoID_getter():
    youtube = build('youtube', 'v3', developerKey=api_key)

    # Read file with video_ids and take first video ID. If file is empty assign default ID.
    f = open("video_id.txt", "r")
    wholeFile = f.read()
    if (wholeFile != ""):
        beggining_video = wholeFile.split()[0][0:]
    else:
        beggining_video = 'RE_ngo5na_8'
    f.close()

    # Add all IDS already collected to list named fifo.
    fifo = []
    final_list = []
    splitFile = wholeFile.split()
    for i in range(0, len(splitFile)):
        fifo.append(splitFile[i])

    # Find related video to last video in list fifo.
    f = open("video_id.txt", "a")
    x = len(fifo) - 1
    for j in range(0, 100):
        request = youtube.search().list(
            part="snippet",
            relatedToVideoId=fifo[x],
            type="video",
            maxResults='50'
        )
        response = request.execute()
        final_list.append(fifo[x])
        fifo.pop(x)
        for i in response['items']:
            if (i['id']['videoId'] not in fifo):
                fifo.append(i['id']['videoId'])
        x = len(fifo) - 1

    # Write final list of IDs to a file.
    for i in range(0, len(fifo)):
        if (fifo[i] not in final_list):
            final_list.append(fifo[i])
    f.truncate(0)
    for i in range(0, len(final_list)):
        f.write(final_list[i] + " ")


# Method to get comments from a video. Maximum of 200 comments.
# Parameter ID represents video id from which we request comments.
# Method returns list of comments.
def comments_getter(id):
    request = youtube.commentThreads().list(
        part="snippet,replies",
        maxResults=200,
        order="orderUnspecified",
        textFormat="plainText",
        videoId=id
    )
    response = request.execute()
    commentsList = []
    for i in range(0, len(response['items'])):
        commentsList.append(response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'])
    return commentsList


# Method for extraction of all necessary video information.
def videoInfo_getter():
    # Repeat  8000 times(limit of one API_key)
    # Everything is in try/except block because not all IDs were still accesible.
    for o in range(0, 8000):
        try:
            # Open file and get first ID.
            f = open("video_id2.txt", "r")
            wholeFile = f.read()
            wholeFile = wholeFile.split()
            id = wholeFile[0]
            f.close()
            f = open("video_id2.txt", "w")
            for a in range(1, len(wholeFile)):
                f.write(wholeFile[a] + " ")

            # Get all information about video with id.
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=wholeFile[0]
            )
            response = request.execute()

            # Extract information. Not every video has tags/comments.
            # Save it to a file.
            info = response['items']
            id = info[0]['id']
            title = info[0]['snippet']['title']
            description = info[0]['snippet']['description']
            if ('tags' in info[0]['snippet']):
                tags = info[0]['snippet']['tags']
            else:
                tags = "[]"
            if (str(info[0].get("statistics").get("commentCount")) == "None") or (
                    str(info[0].get("statistics").get("commentCount")) == '0'):
                listOfComments = []
            else:
                listOfComments = comments_getter(id)
            video_info = "id:" + id + " title: " + title + " " + "description: " + description + " tags: " + str(
                tags) + "comments: " + str(listOfComments) + " "

            f = open("final_dataset.txt", "a", encoding="utf-8")
            f.write(video_info)
            f.close()
        except:
            print(id)


# Method to remove duplicates in case list of IDs has duplicates.
def removeDuplicates():
    f = open("video_id3.txt", 'r')
    wholeFile = f.read()
    wholeFile = wholeFile.split()
    mylist = list(dict.fromkeys(wholeFile))
    f.close()
    f = open("video_id3.txt", "w")
    for i in range(0, len(mylist)):
        f.write(mylist[i] + " ")
    f.close()


# Method that changes .txt file to csv format.
def pysparkCreateCsv():
    spark = SparkSession.builder.master("local").appName("hdfs_test").getOrCreate()
    file = open("test_dataset.txt", 'r', encoding="utf-8")
    listOfVideos = []

    # Split file into videos.
    wholeFile = file.read()
    result = re.split(r"id:", wholeFile)
    for j in range(0, len(result) - 1):
        result[j] = str("id:") + result[j + 1]
    result.pop(len(result) - 1)

    # Parse video information. If video is missing any information video is not added to csv.
    videoTuple = namedtuple("videoTuple", ("id", "title", "description", "tags", "comments"))
    videoTuple.__new__.__defaults__ = (None, None, None, None, None)
    for i in range(0, len(result) - 1):
        if (i % 1000 == 0):
            print(i)
        idIndex = re.search("id:", result[i])
        if (idIndex == None):
            continue
        titleIndex = re.search("title:", result[i])
        if (titleIndex == None):
            continue
        descriptionIndex = re.search("description:", result[i])
        if (descriptionIndex == None):
            continue
        tagsIndex = re.search("tags:", result[i])
        if (tagsIndex == None):
            continue
        commentsIndex = re.search("comments:", result[i])
        if (commentsIndex == None):
            continue

        # Positions where were individual things found.
        idIndexStart = idIndex.span()[1]
        idIndexEnd = titleIndex.span()[0] - 1
        titleIndexEnd = titleIndex.span()[1] + 1
        descriptionIndexStart = descriptionIndex.span()[0] - 1
        descriptionIndexEnd = descriptionIndex.span()[1] + 1
        tagsIndexStart = tagsIndex.span()[0] - 1
        tagsIndexEnd = tagsIndex.span()[1] + 1
        commentIndexStart = commentsIndex.span()[0]
        commentIndexEnd = commentsIndex.span()[1] + 1
        endOfVideoText = len(result[i])

        # Create a tupple and add it to list.
        pre = videoTuple(result[i][idIndexStart:idIndexEnd],
                         result[i][titleIndexEnd:descriptionIndexStart],
                         result[i][descriptionIndexEnd:tagsIndexStart],
                         result[i][tagsIndexEnd:commentIndexStart],
                         result[i][commentIndexEnd:endOfVideoText])
        listOfVideos.append(pre)

    # Create dataframe from list and save it as csv.
    df = spark.createDataFrame(listOfVideos, ["id", "title", "description", "tags", "comments"])
    df.write.csv("test_dataset_preprocessed")


# Read csv and index keywords.
def pysparkReadCsv():
    spark = SparkSession.builder.master("local").appName("hdfs_test").getOrCreate()
    videoSchema = StructType() \
        .add("id", "string") \
        .add("title", "string") \
        .add("description", "string") \
        .add("tags", "string") \
        .add("comments", "string")

    videoData = spark.read.option("multiLine", True).option("encoding", "UTF-8").csv("test_dataset_preprocessed",
                                                                                     schema=videoSchema)
    kw_model = KeyBERT()

    # This method runs for every row in a dataframe.
    def findKeywordsDataframe(x):
        # Create folders if they dont exist.
        if not os.path.isdir("indexes_title"):
            os.mkdir("indexes_title")
        if not os.path.isdir("indexes_desc"):
            os.mkdir("indexes_desc")
        if not os.path.isdir("indexes_comment"):
            os.mkdir("indexes_comment")
        if (x["index"] % 1000 == 0):
            print(x["index"])

        # If title and description and comments arent empty extract keywords.
        if (x.title != None and x.description != None and x.comments != None):
            titleKeywords = kw_model.extract_keywords(x.title)
            descriptionKeywords = kw_model.extract_keywords(x.description)
            commentsKeywords = kw_model.extract_keywords(x.comments)

            # For each keyword create a file or append it to existing file.
            for rt in range(0, len(titleKeywords)):
                file = open("indexes_title/" + titleKeywords[rt][0], 'a+')
                file.write(x.id + "\n")
            for rt in range(0, len(descriptionKeywords)):
                file = open("indexes_desc/" + descriptionKeywords[rt][0], 'a+')
                file.write(x.id + "\n")
            for rt in range(0, len(commentsKeywords)):
                file = open("indexes_comment/" + commentsKeywords[rt][0], 'a+')
                file.write(x.id + "\n")

    df_index = videoData.select("*").withColumn("index", monotonically_increasing_id())
    rddTitle = df_index.rdd.map(findKeywordsDataframe)
    rdT = rddTitle.collect()


def find_keyword_in_files(place, prompt):
    try:
        lines = list()
        file = open(place + "/" + prompt, "r")
        for line in file:
            print(line)
            lines.append(line)
        return lines
    except Exception as e:
        print("keyword " + prompt + " does not exist in any videos")
        return "keyword " + prompt + " does not exist in any videos"


# Method that returns video ids of videos that contain keyword.
# In user input first word is category title,description or comments and second word is searched keyword.
def searchInIndex():
    prompt = [""]
    while (prompt[0] != "exit"):
        print("Enter keyword: ", end="")
        inp = input()
        print(type(inp))
        print(inp)
        prompt_split = inp.split(' ')
        prompt = prompt_split[1]
        place = "indexes_comment" if "comments" in prompt_split[0] else "indexes_desc" if "description" in prompt_split[
            0] else "indexes_title"
        print("Video id's with tag '" + prompt + "' in category '" + prompt_split[0] + "'.")
        find_keyword_in_files(place, prompt)


def testOfSearchInIndex(prompt, testNumber):
    inp = prompt
    prompt_split = inp.split(' ')
    prompt = prompt_split[1]
    place = "indexes_comment" if "comments" in prompt_split[0] else "indexes_desc" if "description" in prompt_split[
        0] else "indexes_title"
    if (testNumber == 4 or testNumber == 5):
        return place
    keyword = find_keyword_in_files(place, prompt)
    return keyword


# def searchTest4():
#     #check parsing

# def searchTest5():
#     #Check if directories are there

# def searchTest6():


def searchTest1():
    testList = ["450p7goxZqg\n"]
    sample = testOfSearchInIndex("comments artist", 1)
    if (sample == testList):
        print("Correct")
        return 1
    else:
        print("Incorrect")
        return 0


def searchTest2():
    sample = testOfSearchInIndex("titles aaaaaaaaaa", 2)
    if (sample == "keyword aaaaaaaaaa does not exist in any videos"):
        print("Correct")
        return 1
    else:
        print("Incorrect")
        return 0


def searchTest3():
    testList = ["b5Ek9F-dnwA\n", "NTk_kTVO0x4\n", "NjDP3dXGQV8\n"]
    sample = testOfSearchInIndex("description 2021", 3)
    if (sample == testList):
        print("Correct")
        return 1
    else:
        print("Incorrect")
        return 0


def searchTest0():
    if os.path.isdir("indexes_title") and os.path.isdir("indexes_comment") and os.path.isdir("indexes_desc"):
        print("Correct")
        return 1
    else:
        print("Incorrect")
        return 0


def searchTest4():
    place = testOfSearchInIndex("description 2021", 4)
    if (place == "indexes_desc"):
        print("Correct")
        return 1
    else:
        print("Incorrect")
        return 0


def searchTest5():
    place = testOfSearchInIndex("aaaaaa 2021", 5)
    print(place)
    if (place == "indexes_title"):
        print("Correct")
        return 1
    else:
        print("Incorrect")
        return 0


def searchTests():
    # Test if indexes exist
    test0 = searchTest0()
    # Tests if search returns correct output
    test1 = searchTest1()
    test2 = searchTest2()
    test3 = searchTest3()
    # Tests if valid category is choosen
    # Default should be title if invalid
    test4 = searchTest4()
    test5 = searchTest5()

    if (test0 == 1 and test1 == 1 and test2 == 1 and test3 == 1 and test4 == 1 and test5 == 1):
        print("All tests correct")
    else:
        print("Something wrong")


def main():
    # Get list of IDs.
    # videoID_getter()

    # Based on IDs get necessary video information. Return that to .txt file.
    # videoInfo_getter()

    # Create .csv file from .txt CSV file is saved to hadoop.
    # pysparkCreateCsv()

    # Read csv and create index from keywords.
    # pysparkReadCsv()

    # Find video ID for keyword.
    # searchInIndex()

    # TESTS
    searchTests()


if __name__ == "__main__":
    main()