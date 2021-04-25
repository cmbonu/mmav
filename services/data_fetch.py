import requests
from bs4 import BeautifulSoup

html = requests.get('https://www.mmafighting.com/schedule')
soup = BeautifulSoup(html.text ,'html.parser')

for a,dt in zip(soup.find_all('h2'),soup.select('.m-mmaf-pte-event-list')[0].find_all('h3')):
    links = a.find_all('a')
    print(links[0].contents[0],' : ',dt.contents[0],' : ',links[0].get('href'))
#print(html.text)
#print(soup.prettify())
