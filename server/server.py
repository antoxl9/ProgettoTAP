from flask import Flask,jsonify
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import JSONFormatter
import time
import json
import os
import re 

iter=0

#LETTURA LISTA.TXT
here = os.path.dirname(os.path.abspath(__file__))  
filename = os.path.join(here, 'lista.txt')
with open(filename) as file:     
   lines = [line.rstrip() for line in file]


#SERVER FLASK
app = Flask(__name__)
 

#ESTRAZIONE VIDEO_ID
def extract_video_id(url):
    
    return url.split("?v=")[1].split("&")[0]
    
    


#API TRASCRIZIONE

def youtube_api(video_id):
    
   #video_id = "_QjJtWenTRs"
   transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=['en'])
   testo=[]
   
   for i in range(len(transcript)): 
    if "[" not in transcript[i]["text"]: 
     testo.append(transcript[i]["text"])
   
   return " ".join(testo)
   

@app.route('/')

# STAMPA DI TUTTI I TESTI

def root():
   risultato={}
   for i in range(len(lines)):
      index="text-" + str(i)
      risultato[index]=youtube_api(extract_video_id(lines[i]))

   return risultato
        
@app.route('/get_trascrizione') 

#STAMPA UN TESTO

def get_trascrizione():
   global iter
   risultato= youtube_api(extract_video_id(lines[iter]))
   iter=iter+1
   #print("sono arrivato qui", iter)

   if iter == 9:  #fare in modo di renderlo automatico, e non creare copie
      iter = 0

   return str(risultato)
 


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)




