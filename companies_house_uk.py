import re
from bs4 import BeautifulSoup

import requests
base_url = "https://beta.companieshouse.gov.uk/company/00045916/officers"
page = requests.get(base_url)
data = page.text
soup = BeautifulSoup(data, "lxml")


datanastere = re.compile('officer-date-of-birth.*')
officername = re.compile('officer-name-.*')
# for links in soup.find_all("div", class_="appointments-list"):
# for headingu in soup.find_all('h2',class_="heading-medium"):
        # for linkuri in soup.find_all('a'):
            # print (linkuri.text)
for zile in soup.find_all('dd',id=datanastere)[0].text:
        # print(zile)
        for ofiteri in soup.find_all('span',id=officername)[0].text:

            file = open('output.csv', 'a')
            for entitati in zile:
                entitati = str(zile)+" , "+str(ofiteri)

                # print(entitati)
                file.write(entitati + ", \n")