from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time, json, nltk
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
plt.style.use('ggplot')




"Must Run Cell One by One"
"Streaming First, Then Load to DataFrame"



#%% get access keys from twitter api account


ckey="XXX"
csecret="XXX"
atoken="XXX"
asecret="XXX"


# how long to streaming, location to save data, filter word
process_time=100 # in seconds
save_name='streaming_temp.txt'
tw_track_name=['stock']




start=time.time() # record current time so that the process can stop after <process_time>
global counter
counter=1
print('cleaning old file...')
file= open(save_name,'w').close() #clear file content


class MyStreamListener(StreamListener):
    # save tweets to <save_name>
    def on_data(self,data):      
        file= open(save_name,'a')
              
        if (time.time()-start)<process_time:  # check time    
            file.write(data)  
            file.write('\n')       
            
            global counter
            print('counter: {}'.format(counter))          
            counter+=1            
            return True  
        
        else:
            print('catching is done!')
            file.close()
            return False
                    

        
        
auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth,MyStreamListener())
twitterStream.filter(track=tw_track_name,languages=['en'], async=True)



  
  
  
#%% load streaming_temp.txt to dataframe


# load afinn word and score to dictionary
afinnfile = open('AFINN-111.txt')
scores = {} 
for line in afinnfile:
  term, score  = line.split("\t")  
  scores[term] = int(score) 


# select key/feature in each tweet which will be loaded to dataframe
location=[]  
name=['text','place','lang']# create a list of feature
for n in name:
    vars()[n]=[]


# loaded to dataframe
with open(save_name,'r') as raw:
    for line in raw :
        if line!='\n': # skip empty rows
            temp=json.loads(line)
            if "created_at" in temp.keys(): # only tweets begin with "created_at" are useful               
                for col in name:  # use loop to creat a list of data for each feature                 
                    vars()[col].append(temp[col])
                location.append(temp['user']['location']) # 'location' is inside 'user' feature, can't be looped

df_dict={} # prepare dict for df                   
df_dict['location']=location # 'location' is inside 'user' feature, can't be looped
for n in name:
    df_dict[n]=vars()[n]

df=pd.DataFrame(data=df_dict)



# in df.text: split each sentence and match 
text_lower=df['text'].str.lower()
text_split_lower=text_lower.str.split()
key=scores.keys()
emotion,keywords=[],[]

for row in range(len(text_split_lower)): # loop over each row
    emo,word=[],[]
    for item in text_split_lower[row]:   # loop over each word in a row             
       
        # map afinn score to each word
        if item in key:
            emo.append(scores[item]) # a list of score for each word in afinn file
            word.append(item)        # a list of word in afinn file   
        else:
           emo.append(0)             # 0 score if no match    
    emotion.append(sum(emo)) # sum emotion score for each row/tweet
    keywords.append(','.join(word)) # reshape/ open [] 


# add emotion and kewwords lists to df    
df=df.assign(emotion=emotion)
df=df.assign(keywords=keywords)
df.keywords.replace('',np.nan,inplace=True)



print(df.info())        
print('\n-----------------------\n{0} Tweets in {1} Seconds: '.format(len(emotion), process_time))
print('Emotion Score Per {0} Tweet Is: {1}'.format(tw_track_name,df.emotion.sum()/len(emotion)))
print('(Max = 5, Min = -5)')
print(time.ctime())
print('\n\nScore\tCounts')
print(df.emotion.value_counts())

#%% get word counts & freq


slist=['rt', 'stock'] # remove specific words from counting
stopwords = nltk.corpus.stopwords.words('english')
stopwords=[*stopwords,*slist]


lemm = nltk.stem.WordNetLemmatizer()


all_words = text_lower.str.split(expand=True).unstack().replace('None',np.nan).dropna() # drop 'None'
all_words_lemmatized=[]

# lemmatize words that not in <stopwords>
for word in all_words:
    if word not in stopwords:
        all_words_lemmatized.append(lemm.lemmatize (word))
    


# print top n words
top_n=30        
df_word= pd.DataFrame(data={'word':all_words_lemmatized})
df_word_freq= df_word.word.value_counts()[:top_n].reset_index()
df_word_freq.columns=['word','freq']

print(df_word_freq)












