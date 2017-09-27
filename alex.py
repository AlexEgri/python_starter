# -*- coding: utf-8 -*-
# !/usr/bin/env python

import re
from bs4 import BeautifulSoup
import json

import my_requests

base_url_page1 = 'https://beta.companieshouse.gov.uk/company/00045916/officers?page=1'
base_url_page2 = 'https://beta.companieshouse.gov.uk/company/00045916/officers?page=2'


datanastere = re.compile('officer-date-of-birth.*')
officername = re.compile('officer-name-.*')



def generate_unformatted_entities():
    for url in (base_url_page1, base_url_page2):
        page = my_requests.get(url)
        data = page.content
        soup = BeautifulSoup(data, "lxml")

        content = soup.find('div', {'class': 'appointments-list'}).findAll('div')
        for element in content:
            name = element.find('span', {'id': officername})
            date_of_birth = element.find('dd', {'id': datanastere})

            try:
                name = name.find('a').text
            except:
                continue

            try:
                date_of_birth = date_of_birth.text.strip()
            except:
                continue

            yield {
                'name': name,
                'date_of_birth': date_of_birth
            }


def main():
    file = open('my_data.csv', 'a')
    for entity in generate_unformatted_entities():
        file.write(entity['name'] + "\t" + entity['date_of_birth'] + "\n")
        print(':ACCEPT:{}'.format(json.dumps(entity, ensure_ascii=False)))


if __name__ == '__main__':
    main()
