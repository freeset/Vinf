from keybert import KeyBERT
import json
import pyspark
from langdetect import detect_langs
from googleapiclient.discovery import build
import re
from rake_nltk import Rake
api_key='AIzaSyABGDWyqMQ1xQLwwTgjeNiXYN6J7sMppsg'
#api_key='AIzaSyB5_ETQPRwDzC-9KhGb8S_k2CZRNSNqFGg'
#api_key ='AIzaSyDm_6C77U3HzPTCJpQQk68YBEUCzaTaVz0'
#api_key = 'AIzaSyCAGT53gE1aVQpzBhJlIYiEXXSVpQzu0zI'
youtube = build('youtube','v3',developerKey=api_key)


class video:
    def __init__(self, id, title,description,tags,comments,keywordTitle=0,keywordDesc=0,keywordComm=0,lang=0):
        self.id = id
        self.title = title
        self.description=description
        self.tags=tags
        self.comments=comments
        self.keywordTitle= keywordTitle
        self.keywordDesc= keywordDesc
        self.keywordComm =keywordComm
        self.lang = lang

def videoID_getter():
    #video crawler is starting on
    apiKey_list =[]
    for l in range(0,len(apiKey_list)):
        print(apiKey_list[l])
        youtube = build('youtube', 'v3', developerKey=apiKey_list[l])
        #VYTIAHNUTIE STARTOVACIEHO VIDEA
        f = open("video_id.txt", "r")
        wholeFile = f.read()
        if(wholeFile != ""):
            beggining_video = wholeFile.split()[0][0:]
        else:
            beggining_video = 'RE_ngo5na_8'
        f.close()

        fifo = []
        final_list = []
        splitFile = wholeFile.split()
        #nacitanie vsetko do fifa
        for i in range(0,len(splitFile)):
            fifo.append(splitFile[i])
        print("Je zozbieranych: "+ str(len(fifo))+ " zaznamov")
        f = open("video_id.txt", "a")
        x = len(fifo)-1
        #teraz mam vsetky vo fife, prvy popnem out a zapisem do finalneho
        for j in range(0,100):
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
                #print(response['items'])
                if(i['id']['videoId'] not in fifo):
                    fifo.append(i['id']['videoId'])
            #sleep(randint(1, 3))
            x = len(fifo)-1




        for i in range(0,len(fifo)):
            if(fifo[i] not in final_list):
                final_list.append(fifo[i])
        f.truncate(0)
        for i in range(0,len(final_list)):
            f.write(final_list[i]+" ")

def comments_getter(id):
    #print(id)
    request = youtube.commentThreads().list(
        part="snippet,replies",
        maxResults=200,
        order="orderUnspecified",
        textFormat="plainText",
        videoId=id
    )
    response = request.execute()
    commentsList = []
    for i in range(0,len(response['items'])):
        #print(len(response['items']))
        commentsList.append(response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'])
    return commentsList

def videoInfo_getter():
    for o in range(0,8000):
        try:
            print("B")
            f = open ("video_id2.txt","r")
            wholeFile = f.read()
            wholeFile = wholeFile.split()
            id = wholeFile[0]
            f.close()
            print("C")
            f = open("video_id2.txt","w")
            for a in range(1,len(wholeFile)):
                f.write(wholeFile[a]+" ")

            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=wholeFile[0]
                #id = 'RE_ngo5na_8'
            )
            response = request.execute()
            print("A")
            info = response['items']
            id = info[0]['id']
            title = info[0]['snippet']['title']
            description = info[0]['snippet']['description']
            if('tags' in info[0]['snippet']):
                tags = info[0]['snippet']['tags']
            else:
                tags = "[]"
            if(str(info[0].get("statistics").get("commentCount"))=="None") or (str(info[0].get("statistics").get("commentCount"))=='0'):
               listOfComments=[]

            else:

                listOfComments = comments_getter(id)
            video_info = "id:"+id+" title: " + title + " " + "description: " + description + " tags: " + str(tags) + "comments: " + str(listOfComments)+" "

            f = open("final_dataset.txt", "a",encoding="utf-8")
            f.write(video_info)
            f.close()
        except:
            print(id)
            print("Ok")
def removeDuplicates():
    f=open("video_id3.txt",'r')
    wholeFile = f.read()
    wholeFile = wholeFile.split()
    mylist = list(dict.fromkeys(wholeFile))
    f.close()
    f = open("video_id3.txt", "w")
    for i in range(0,len(mylist)):
        f.write(mylist[i]+" ")
    f.close()
def findKeyWords():
    print("A")
    rake = Rake()
    pocetIteracii=20
    f = open("final_dataset.txt", 'r',encoding="utf-8")
    list = []

    wholeFile = f.read()
    result = re.split(r"id:", wholeFile)

    for j in range(0,len(result)-1):
        result[j] = str("id:") + result[j+1]

    result.pop(len(result)-1)



    for i in range (0,pocetIteracii):
        a= re.search("id:", result[i])
        b = re.search("title:",result[i])
        f = re.search("description:",result[i])
        h = re.search("tags:",result[i])
        j = re.search("comments:", result[i])
        c=a.span()[1]
        d=b.span()[0]-1
        e = b.span()[1] + 1
        g = f.span()[0]-1
        k= f.span()[1]+1

        l = h.span()[0]-1
        r=h.span()[1]+1
        o=j.span()[0]
        s=j.span()[1]+1
        t=len(result[i])



        pre = video(result[i][c:d], result[i][e:g],result[i][k:l],result[i][r:o],result[i][s:t])
        list.append(pre)

    #TITLE
    for i in range (0,pocetIteracii):
        print(i)
        try:
            lan={lang.lang: lang.prob for lang in detect_langs(list[i].description)}
            if "en" in lan:
                if(lan["en"]>0.8):
                    list[i].lang=1
        except:
            list[i].lang =0

    for i in range(0,len(list)):
        print(i)
        if(list[i].lang==1):
            kw_model = KeyBERT()
            keywords = kw_model.extract_keywords(list[i].description)
            list[i].keywordDesc = []
            for it in range(0,len(keywords)):
                list[i].keywordDesc.append(keywords[it][0])

            keywords = kw_model.extract_keywords(list[i].title)
            list[i].keywordTitle = []
            for rt in range(0, len(keywords)):
                list[i].keywordTitle.append(keywords[rt][0])

            keywords = kw_model.extract_keywords(list[i].comments)
            #print(list[i].comments)
            list[i].keywordComm =[]
            for rf in range(0, len(keywords)):
                list[i].keywordComm.append(keywords[rf][0])


    indexT = dict()
    indexD = dict()
    indexC = dict()
    for i in range(0,len(list)):
        if(list[i].keywordTitle!= 0):
            for j in range(0,len(list[i].keywordTitle)):
                if(list[i].keywordTitle[j] in indexT):
                    indexT[list[i].keywordTitle[j]].append(list[i].id)
                else:
                    indexT[list[i].keywordTitle[j]]= [list[i].id]
        if (list[i].keywordDesc != 0):
            for j in range(0, len(list[i].keywordDesc)):
                if (list[i].keywordDesc[j] in indexD):
                    indexD[list[i].keywordDesc[j]].append(list[i].id)
                else:
                    indexD[list[i].keywordDesc[j]] = [list[i].id]
        if (list[i].keywordComm != 0):
            for j in range(0, len(list[i].keywordComm)):
                if (list[i].keywordComm[j] in indexC):
                    indexC[list[i].keywordComm[j]].append(list[i].id)
                else:
                    indexC[list[i].keywordComm[j]] = [list[i].id]


    #Otvorenie description indexu a načítanie
    d = open("indexD.txt", "w")
    d.write(json.dumps(indexD))
    d.close()
    # Otvorenie title indexu a načítanie

    d = open("indexT.txt", "w")
    d.write(json.dumps(indexT))
    d.close()

    # Otvorenie comment indexu a načítanie

    d = open("indexC.txt", "w")
    d.write(json.dumps(indexC))
    d.close()

    print(a)
    print(b)
    print(c)

    print("Ak chceš vyhľadavať v Title stlač 1 ")
    print("Ak chceš vyhľadávať v Popiskoch stlač 2")
    print("Ak chceš vyhľadávať v Commentoch stlač 2")
#    d= open("indexD.txt","r")
#    a= json.loads(d.read())






def getIdFromJson():
    print("A")
    f = open('data.txt')
    data= f.readlines()
    f.close()
    f= open('video_id2.txt','w')
    for i in range(1,len(data)):
        #print(data[i])
        f.write(data[i].split(',')[0]+" ")
        print(data[i].split(',')[0])

def search():
    # Otvorenie description indexu a načítanie
    d = open("indexD.txt", "r")
    a = json.loads(d.read())
    d.close()

    # Otvorenie title indexu a načítanie
    d = open("indexT.txt", "r")
    b = json.loads(d.read())
    d.close()

    # Otvorenie comment indexu a načítanie
    d = open("indexC.txt", "r")
    c = json.loads(d.read())
    d.close()

    print(a)
    print(b)
    print(c)

    print("Ak chceš vyhľadavať v Title stlač 1 ")
    print("Ak chceš vyhľadávať v Popiskoch stlač 2")
    print("Ak chceš vyhľadávať v Commentoch stlač 3")
    input1 = input()
    if(input1 =='1'):
        print("Zadaj hľadaný výraz.")
        while(True):
            input1 = input()
            if(input1 in a):
                print(a[input1])
            else:
                print("Keyword nie je v indexe.")
    elif(input1 == '2'):
        print('Zadaj hľadaný výraz.')
        while (True):
            input1 = input()
            if (input1 in b):
                print(b[input1])
            else:
                print("Keyword nie je v indexe.")
    elif(input1 == '3'):
        print('Zadaj hľadaný výraz.')
    else:
        print("Missclick")

def main():
    videoInfo_getter()
    #findKeyWords()
    #removeDuplicates()
    #videoID_getter()
    #getIdFromJson()
    #search()

if __name__ == "__main__":
    main()