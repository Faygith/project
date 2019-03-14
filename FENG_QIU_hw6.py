import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
import json
import csv,sqlite3
import re
import pandas as pd
import FENG_QIU_hw5_csvmodule


def source1_data(): #scrape boxoffice top 100  movies released in 2018 from the first data source: boxofficemojo
	page = requests.get('http://www.boxofficemojo.com/yearly/chart/?view=releasedate&view2=domestic&yr=2018&sort=gross&order=DESC&p=.htm')
	soup = BeautifulSoup(page.text,'html.parser')

	main_table = soup.findAll('table',{"cellpadding" : "5"})[0] 
	movie_info = []
	for each in main_table.findAll('tr'):
		if (len(each.findAll('td')) > 8): #get rid of the first 'td' which doesn't include movie info
		    movie_name = each.findAll('td')[1].find('a')
		    movie_name = movie_name.string
		        
		    studio = each.findAll('td')[2].find('font')
		    studio = studio.string
		        
		    total_gross = each.findAll('td')[3].find('b')
		    total_gross = total_gross.string[1:].replace(',','')
		    total_gross = int(total_gross)
		        
		    open_date = each.findAll('td')[7].find('a')
		    open_date = open_date.string
		        
		    movie_info.append((movie_name,studio,total_gross,open_date)) # add movies informations into a list
	FENG_QIU_hw5_csvmodule.create_csv('FENG_QIU_hw5_source1.csv', movie_info)#create a csv file to store the result of this source


def source2_data(): #get more movie data from the second source: TMDB API
	#get the movie list from soure1 
	page = requests.get('http://www.boxofficemojo.com/yearly/chart/?view=releasedate&view2=domestic&yr=2018&sort=gross&order=DESC&p=.htm')
	soup = BeautifulSoup(page.text,'html.parser')
	main_table = soup.findAll('table',{"cellpadding" : "5"})[0] 
	movie_info = []
	for each in main_table.findAll('tr'):
		if (len(each.findAll('td')) > 8): 
		    movie_name = each.findAll('td')[1].find('a')
		    movie_name = movie_name.string
		    studio = each.findAll('td')[2].find('font')
		    studio = studio.string
		    total_gross = each.findAll('td')[3].find('b')
		    total_gross = total_gross.string  
		    open_date = each.findAll('td')[7].find('a')
		    open_date = open_date.string   
		    movie_info.append((movie_name,studio,total_gross,open_date)) 

	more_info=[] #create a list for data from source2
	for each in movie_info:
	    pattern = re.search(r"\(201[0-9]\)",each[0])
	    if pattern: # some movie names have "(2018/2017)" at the end, which will affect the search through API
	        removed = re.sub(r"\(201[0-9]\)","",each[0]) #remove those years
	        name = quote(removed) # add '%20' in movie name for url
	    else: 
	    	name = quote(each[0]) 

	    url = "https://api.themoviedb.org/3/search/movie?api_key=059b49cb425221e561d277848123db0a&language=en-US&query="+name
	    payload = "{}"
	    response = requests.request("GET", url, data=payload)
	    content = response.text
	        
	    popularity_po = content.find('popularity')
	    poster_path_po = content.find('poster_path')
	    overview_po = content.find('overview')
	    date_po = content.find('release_date')
	    genre_po = content.find('genre_ids')
	    path_po = content.find('backdrop_path')

	    popularity = content[popularity_po+12:poster_path_po-2]
	    overview = content[overview_po+11:date_po-3 ]
	    genre = content[genre_po+12:path_po-3]      
	    more_info.append((each[0],popularity,overview,genre)) #add all data from source2 into a list
	FENG_QIU_hw5_csvmodule.create_csv('FENG_QIU_hw5_source2.csv',more_info) #wrtie the data into a csv file
	
def source3_data():# scrape top100 movies' tomatometers and audience scores from source3: rotten tomatoes
	#get the movie list we need from source1
	page = requests.get('http://www.boxofficemojo.com/yearly/chart/?view=releasedate&view2=domestic&yr=2018&sort=gross&order=DESC&p=.htm')
	soup = BeautifulSoup(page.text,'html.parser')
	main_table = soup.findAll('table',{"cellpadding" : "5"})[0] 
	movie_info = []
	for each in main_table.findAll('tr'):
		if (len(each.findAll('td')) > 8): 
		    movie_name = each.findAll('td')[1].find('a')
		    movie_name = movie_name.string
		    studio = each.findAll('td')[2].find('font')
		    studio = studio.string 
		    total_gross = each.findAll('td')[3].find('b')
		    total_gross = total_gross.string   
		    open_date = each.findAll('td')[7].find('a')
		    open_date = open_date.string   
		    movie_info.append((movie_name,studio,total_gross,open_date))

	movie_tomato=[] #create a list for data from soure3
	for each in movie_info:
	    pattern = re.search(r"\(201[0-9]\)",each[0])
	    if pattern:
	        removed = re.sub(r"\(201[0-9]\)","",each[0]) #remove the extra characters "(2018/2017)" from movie names
	        url = "_".join( removed.split() ) #add underscore in movie name for url
	    else:
	        pattern = re.search(r"[!\',.:]",each[0]) #use regex to find and remove special characters from movie names
	        if pattern:
	            url = re.sub('[!\',.:]','',each[0])
	            url = "_".join(url.split() ) 
	        else:
	            url = "_".join(each[0].split() )

	    try : #two kinds of urls of movie pages
	        page = requests.get('https://www.rottentomatoes.com/m/'+url+'_2018') #series movie has '_2018' behind their name in url
	        soup = BeautifulSoup(page.text,'html.parser')
	        tomato = soup.find('span',class_= 'meter-value superPageFontColor')
	        tomato_meter = tomato.get_text()

	    except :
	        page = requests.get('https://www.rottentomatoes.com/m/'+url) 
	        soup = BeautifulSoup(page.text,'html.parser')        
	        tomato = soup.find('span',class_= 'meter-value superPageFontColor')
	        if tomato:
	            tomato_meter = tomato.get_text()
	        else:# not all movies from the list have valid scores on rotten tomatoes
	        	movie_tomato.append((each[0], 'null','null')) # I still want the movie name in the list so I add "null" when there's no scores.
	        	continue

	    audience = soup.find('span',class_='superPageFontColor',style='vertical-align:top')
	    au_score = audience.get_text()
	    movie_tomato.append((each[0], tomato_meter,au_score))
	FENG_QIU_hw5_csvmodule.create_csv('FENG_QIU_hw5_source3.csv',movie_tomato) #write data from source3 into a csv file


def source1sql():# create a table for source1 data 
	conn = sqlite3.connect('HW5.db') #create a new database 'HW5'
	cur = conn.cursor()
	cur.execute("DROP TABLE IF EXISTS topgrossmovie ") #create the table 'topgrossmovie for source1'
	cur.execute("""CREATE TABLE topgrossmovie(
					ranking integer PRIMARY KEY,
					movie_name text NOT NULL,
					studio text NOT NULL,
					total_gross real NOT NULL,
					open_date real NOT NULL); """)
	conn.close()
	print('source1 table is created') # print out the message so we know the table has been created


def source2sql():# create a table for source2 data
	conn = sqlite3.connect('HW5.db')
	cur = conn.cursor()
	cur.execute("DROP TABLE IF EXISTS moreinfo") #create the table 'moreinfo' for source2
	cur.execute("""CREATE TABLE moreinfo (
					movie_name text ,
					popularity real NOT NULL,
					overview text NOT NULL,
					genre_id integer NOT NULL);""")
	conn.close()
	print('source2 table is created')

def source3sql():# create a table for source3 data
	conn = sqlite3.connect('HW5.db')
	cur = conn.cursor()
	cur.execute("DROP TABLE IF EXISTS tomato") #create the table 'tomato' for source3
	cur.execute("""CREATE TABLE tomato(
					movie_name text ,
					tomato_meter real NOT NULL,
					audience_score real NOT NULL); """)
	conn.close()
	print('source3 table is created')


def studio(): 
	conn = sqlite3.connect('HW5.db')
	sg= pd.read_sql_query('select topgrossmovie.studio,topgrossmovie.total_gross from topgrossmovie', conn)
	studio = sg.groupby('studio')['total_gross'].agg(['sum','count'])# calculate the number of movies and total grosses of different studios 
	#print(studio)
	summax = studio.loc[studio['sum'].idxmax()] #find the most profitable studio
	countmax = studio.loc[studio['count'].idxmax()] # find the most productive studio
	print(summax)
	print(countmax)
	print("--------------------------------------------------------------------------------")
	print("Conclusion for Q1:")
	print("'BV'(the Walt Disney Studios) has the highest box office with $1,155,791,000 box office and 3 movies")
	print("'Fathom' had 9 movies released with $11,033,841 box office ")
	

def lowestscore(): 
	conn = sqlite3.connect('HW5.db')
	avs= pd.read_sql_query('''select tomato.tomato_meter,tomato.audience_score 
								from tomato''', conn)
	avs['tomato_meter'] = avs['tomato_meter'].str.replace('%','') #remove the percent signs in the data
	avs['audience_score'] = avs['audience_score'].str.replace('%','')
	avs.tomato_meter= pd.to_numeric(avs.tomato_meter,errors = 'coerce')	# change string to integer
	avs.audience_score= pd.to_numeric(avs.audience_score,errors = 'coerce')
	avs['mean'] = avs.mean(axis=1) #add another column called 'mean'  
	low = avs.loc[avs['mean'].idxmin()] #find the lowest score movie
	print(low)
	print("--------------------------------------------------------------------------------")
	print("Conclusion for Q2:")
	print("'Truth or Dare' has the lowest average score with 15% tomato_meter and 19% audience score")
	print("The reason why it still has a pretty good box office performance(No.23) maybe because of the movie cast and genre")


def correlation():
	#Using two methods to generate conclusion: calculate the correlation and see the rankings of high popularity movies.
	conn = sqlite3.connect('HW5.db')
	relation = pd.read_sql_query('''select topgrossmovie.total_gross, moreinfo.popularity
								from topgrossmovie
								inner join moreinfo 
								on topgrossmovie.movie_name = moreinfo.movie_name  ''', conn)
	relation.popularity= pd.to_numeric(relation.popularity,errors = 'coerce')	# change string to integer
	value =  relation['total_gross'].corr(relation['popularity']) # calculate the correlation, the correlation is stronger when the value is close to 1
	#print(value)

	hr = pd.read_sql_query('''select topgrossmovie.ranking, moreinfo.popularity
								from topgrossmovie
								inner join moreinfo 
								on topgrossmovie.movie_name = moreinfo.movie_name
								  ''', conn)
	hr.popularity = pd.to_numeric(hr.popularity,errors = 'coerce')
	highpop = hr.loc[hr['popularity'] > 100] #find all the movies that have high popularity(>100) and check their ranking 
	print(highpop)
	print("---------------------------------------------------------------------------------")
	print('Conslusion for Q3:')
	print('''The value of correlation is 0.7 which indicated the correlation between high popularity and
high box office. In addition, all movies that have over 100 popularity are in top10 movie in ranking.
Thus the high popularity can reflect high box office''')


def preference():
	conn = sqlite3.connect('HW5.db')
	pre = pd.read_sql_query('''select  moreinfo.genre_id,tomato.tomato_meter,tomato.audience_score 
								from moreinfo
								inner join tomato
								on moreinfo.movie_name = tomato.movie_name  ''', conn)	
	pre['tomato_meter'] = pre['tomato_meter'].str.replace('%','')
	pre['audience_score'] = pre['audience_score'].str.replace('%','')
	pre.tomato_meter= pd.to_numeric(pre.tomato_meter,errors = 'coerce')	# change string to integer
	pre.audience_score= pd.to_numeric(pre.audience_score,errors = 'coerce')
	hightmt = pre.loc[pre['tomato_meter'] > 90] #find all movies have bigger than 90% tomato meter 
	hightas = pre.loc[pre['audience_score'] > 90] #find all movies have bigger than 90% audience score
	print(hightmt)
	print(hightas)
	print("---------------------------------------------------------------------------------")
	print('Conclusion for Q4:')
	print('''By looking at the genres of movies that have 90% and more scores from tomato meter and audience
score respectively, I found that the both tomato critics and general audience are inclinded to
give high score for Action/Adventure/Animation/Comedy/Drama/Fantasy/Family/Science Fiction/Romance movies.
While critics would prefer Documentary/Horror/Thriller/History/Music movies,and general 
audience would prefer Crime movies.''')


if sys.argv[1]=="source=remote": # get the data from website or API
	source1_data()
	print("source1 csv file is created") #print out the message when csv files are created 
	source2_data()
	print("source2 csv file is created")
	source3_data()
	print("source3 csv file is created")

	#write all data from csv file to sqlite
	source1sql()
	conn = sqlite3.connect('HW5.db')
	cur = conn.cursor()
	with open('FENG_QIU_hw5_source1.csv','r') as s1 :
		reader = csv.reader(s1)
		for row in reader:
			cur.execute('''INSERT INTO topgrossmovie (movie_name,studio,total_gross,open_date) 
				           VALUES (?,?,?,?)''',row)
	s1.close()
	conn.commit()
	conn.close()

	source2sql()
	conn = sqlite3.connect('HW5.db')
	cur = conn.cursor()
	with open('FENG_QIU_hw5_source2.csv','r') as s2 :
		reader = csv.reader(s2)
		for row in reader:
			cur.execute('''INSERT INTO moreinfo (movie_name,popularity,overview,genre_id) 
				           VALUES (?,?,?,?)''',row)
	s2.close()
	conn.commit()
	conn.close()

	source3sql()
	conn = sqlite3.connect('HW5.db')
	cur = conn.cursor()
	with open('FENG_QIU_hw5_source3.csv','r') as s3 :
		reader = csv.reader(s3)
		for row in reader:
			cur.execute('''INSERT INTO tomato (movie_name,tomato_meter,audience_score) 
				           VALUES (?,?,?)''',row)
	s3.close()	
	conn.commit()
	conn.close()


if sys.argv[1] == "source=local":# get access to data from local
	# instead of creating the csv file,open them when I wamt to wrtie the content into database
	source1sql()
	conn = sqlite3.connect('HW5.db')
	cur = conn.cursor()
	with open('FENG_QIU_hw5_source1.csv','r') as s1 :
		reader = csv.reader(s1)
		for row in reader:
			cur.execute('''INSERT INTO topgrossmovie (movie_name,studio,total_gross,open_date) 
				           VALUES (?,?,?,?)''',row)
	s1.close()
	conn.commit()
	conn.close()

	source2sql()
	conn = sqlite3.connect('HW5.db')
	cur = conn.cursor()
	with open('FENG_QIU_hw5_source2.csv','r') as s2 :
		reader = csv.reader(s2)
		for row in reader:
			cur.execute('''INSERT INTO moreinfo (movie_name,popularity,overview,genre_id) 
				           VALUES (?,?,?,?)''',row)
	s2.close()
	conn.commit()
	conn.close()

	source3sql()
	conn = sqlite3.connect('HW5.db')
	cur = conn.cursor()
	with open('FENG_QIU_hw5_source3.csv','r') as s3 :
		reader = csv.reader(s3)
		for row in reader:
			cur.execute('''INSERT INTO tomato (movie_name,tomato_meter,audience_score) 
				           VALUES (?,?,?)''',row)
	s3.close()	
	conn.commit()
	conn.close()

if sys.argv[1] == "result": # one result is to join all three tables together
	if sys.argv[2] == 'Question1':
		print("*****Question 1: Which studio has the highest box office and which has the most movies?*****")
		print("---------------------------------------------------------------------------------")
		print('Data analyzing for Q1:')
		studio()
	if sys.argv[2] == 'Question2':
		print("*****Question 2: What's the movie with the lowerest average score? why do you think it still has high box office?*****")
		print("---------------------------------------------------------------------------------")
		print('Data analyzing for Q2:')
		lowestscore()
	if sys.argv[2] == 'Question3':	
		print("*****Question 3: Does high popularity reflect the high box office performance?*****")
		print("---------------------------------------------------------------------------------")
		print('Data analyzing for Q3:')
		correlation()
	if sys.argv[2] == 'Question4':
		print("*****Question 4: Do tomato critics and audience prefer different movie genres?*****")
		print("---------------------------------------------------------------------------------")
		print('Data analyzing for Q4:')
		preference()






































