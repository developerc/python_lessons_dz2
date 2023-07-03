import  requests
from bs4 import BeautifulSoup
from time import sleep
import json
import sqlite3
from sqlite3 import Error

def handle_request(url):
    listLinks = []
    user_agent = {'User-agent': 'Mozilla/5.0'}
    try:                
        result = requests.get(url, headers=user_agent, timeout=5)
        result.raise_for_status()
        print(result.status_code)
        soup = BeautifulSoup(result.content, 'lxml')
        links = soup.find_all("a")
        for link in links:    
            txt = link.text.lower()
            if "python" in txt and "middle" in txt:
                listLinks.append(link.attrs.get('href'))
                #print(link.text, link.attrs.get('href'))
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        print ("OOps: Something Else",err)
    return listLinks


def handle_single_vacancy(url):
    dict = {}
    user_agent = {'User-agent': 'Mozilla/5.0'}
    try:
        result = requests.get(url, headers=user_agent)
        #print(result.status_code)
        soup = BeautifulSoup(result.content, 'lxml')    
        scripts = soup.find_all("script")
        for script in scripts:           
            if script.attrs.get('type') == "application/ld+json":            
                info = json.loads(script.text)                    
                dict['title'] = info['title']
                #print(dict['title'])            
                dict['name'] = info['hiringOrganization']['name']
                #print(dict['name'])            
                dict['description'] = info['description']
                #print(dict['description'])
                dict['skills'] = ''
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        print ("OOps: Something Else",err)
    return dict

def handle_single_vacancy_api(url):
    dict = {}
    result = requests.get(url)
    print(result.status_code)
    data = json.loads(result.text)
    company_name = data['employer']['name']
    #print(company_name)
    dict['name'] = data['employer']['name']
    dict['title'] = data['name']
    dict['description'] = data['description']
    moreSkills = ''
    key_skills = data['key_skills']
    for skill in key_skills:
        moreSkills = moreSkills + skill['name'] + ", "
    dict['skills'] = moreSkills
    return dict

def add_to_base(table, dict):
    try:
        conn = sqlite3.connect('hh.db')
        conn.execute('CREATE TABLE IF NOT EXISTS '+ table + ' (company_name TEXT, position TEXT, job_description TEXT, key_skills TEXT)')
        query = """INSERT INTO """ + table + """ (company_name, position, job_description, key_skills) VALUES (?,?,?,?)"""
        cursor = conn.cursor()
        value = []
        value.append(dict['name'])
        value.append(dict['title'])
        value.append(dict['description'])
        value.append(dict['skills'])
        valTup = tuple(value)
        cursor.execute(query, valTup)
    except Error:
      print(Error)
    finally:
        if conn:
            conn.commit()
            conn.close() 


def handle_request_api(url): 
    listLinks = []
    try:                   
        result = requests.get(url)
        #print(result.status_code)    
        data = json.loads(result.text)    
        vacancies = data.get('items')
        for vacancy in vacancies:
            txt = vacancy['name'].lower()
            if "python" in txt and "middle" in txt:
                listLinks.append(vacancy['url'])
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        print ("OOps: Something Else",err)
    return listLinks

url = 'https://hh.ru/search/vacancy?no_magic=true&L_save_area=true&text=middle+Python&excluded_text=&customDomain=1&customDomain=2&customDomain=3&area=1001&area=113&area=5&area=40&area=9&area=16&area=28&area=48&area=97&salary=&currency_code=RUR&experience=doesNotMatter&order_by=relevance&search_period=0&items_on_page=50'
fullListLinks = []
i = 1
fullListLinks.extend(handle_request(url))
print(len(fullListLinks))
sleep(2)
while len(fullListLinks) <= 20:        
    fullListLinks.extend(handle_request(url + "&page=" + str(i)))
    print(len(fullListLinks))
    i = i + 1
    sleep(2)
for url in fullListLinks:
    dict = handle_single_vacancy(url)
    print(dict['name'])
    add_to_base("VACANCIES1", dict)
    
print("Работа с API")   
fullListLinksApi = [] 
url = "https://api.hh.ru/vacancies?text=middle%20python&per_page=20&page="
i = 1
while len(fullListLinksApi) <= 100: 
    fullListLinksApi.extend(handle_request_api(url + str(i)))
    i = i + 1
#print(fullListLinksApi)    
for url in fullListLinksApi:
    dict = handle_single_vacancy_api(url)
    print(dict['name'])
    add_to_base("VACANCIES2", dict)