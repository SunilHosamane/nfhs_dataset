# -*- coding: utf-8 -*-
"""
Created on Mon Jul  5 23:27:44 2021

@author: hosam
"""

from bs4 import BeautifulSoup
import requests
import tabula
import pandas as pd
from tabula.io import read_pdf

def get_state_urls():
    #Returns Dict of State name and url
    b=BeautifulSoup(requests.get('http://rchiips.org/nfhs/districtfactsheet_NFHS-5.shtml').text,features="lxml")
    c=b.find_all('option', target='new')
    url=[]
    State=[]
    for x in c:
        url.append('http://rchiips.org/nfhs/'+x.get('value'))
        State.append(x.text)
    d=dict(zip(State,url))
    return d


def get_districts_pdf(state_url):
    
#Returns all districts hrefs for corresponding state url
#params state_url: Href of the state which is to be found out
#Returns Dict of District hrefs and District Names
    soup=BeautifulSoup(requests.get(state_url).text,'lxml')
    a=soup.find_all('option', target='new')
    district_url=[]
    district_name=[]
    for x in a:
        district_url.append('http://rchiips.org/nfhs/'+x.get('value'))
        district_name.append(x.text)
    
    
    district=dict(zip(district_name,district_url))
    return district

def get_data(State, district_name, district_url,location):
    a=tabula.io.read_pdf(district_url, stream=True, pages='all')
    
    try:
        c=pd.concat([a[0],a[1], a[2]])
        d=c.transpose()
        d.columns=d.iloc[0]
        d=d[1:]
    
        d['State']=State
        d['District']=district_name
        print(State, district_name )
        d.to_csv(location, mode='a', header=False)
    except IndexError:
        print ("Not possible for", State, district_name)
        

location='C:/Users/hosam/Documents/Project/NFHS/Test/test1.csv' #path
statesData=get_state_urls()
for state, url in statesData.items():
    district=get_districts_pdf(url) #dict
    for a, b in district.items():
        get_data(state, a, b, location)



    
    