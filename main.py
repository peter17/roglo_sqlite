#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup

import urllib.parse
import requests
import sqlite3
import time
import json
import sys
import re
import os

class DB:
    path = os.path.dirname(os.path.realpath(__file__))
    con = sqlite3.connect(path + '/db.sqlite3')
    cur = con.cursor()

    def update():
        DB.cur.execute('CREATE TABLE IF NOT EXISTS people (firstname TEXT NOT NULL DEFAULT "", lastname TEXT NOT NULL DEFAULT "", sex TEXT, birthdate DATE, birthplace TEXT, deathdate DATE, deathplace TEXT, permalink TEXT PRIMARY KEY, family_id INT, CONSTRAINT `unique_permalink` UNIQUE(permalink) ON CONFLICT REPLACE)')
        DB.cur.execute('CREATE TABLE IF NOT EXISTS family (id TEXT PRIMARY KEY, father_permalink TEXT, mother_permalink TEXT, wedding_date DATE, wedding_place TEXT, CONSTRAINT `unique_id` UNIQUE(id) ON CONFLICT REPLACE)')
        DB.con.commit()


class People:
    def __init__(self):
        self.firstname = self.lastname = self.sex = self.birthdate = self.birthplace = self.deathdate = self.deathplace = self.permalink = ''

    def __str__(self):
        return ' '.join(map(str, (self.sex, self.firstname, self.lastname, self.permalink, self.birthdate, self.deathdate, self.birthplace, self.deathplace)))

    def save(self, DB):
        DB.cur.execute('INSERT INTO people (firstname, lastname, sex, birthdate, birthplace, deathdate, deathplace, permalink) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', (self.firstname, self.lastname, self.sex, self.birthdate, self.birthplace, self.deathdate, self.deathplace, self.permalink))

class Process:
    base = 'http://roglo.eu/roglo?'

    def __init__(self, filename):
        self.filename = filename
        self.cache = {}

    def init_caches(self):
        if not len(self.cache) and os.path.isfile(self.filename) and os.path.getmtime(self.filename) > time.time() - 12 * 3600 and os.path.getsize(self.filename) > 0:
            with open(self.filename, 'r', encoding='utf-8') as f:
                self.cache = json.load(f)

    def save_caches(self):
        with open(self.filename, 'w') as f:
            json.dump(self.cache, f)

    def extractParams(href):
        str1 = Process.extractQuery(href)
        return {x[0] : x[1] for x in [x.split("=") for x in str1[1:].split(";") ]}

    def extractQuery(href):
        parts = href.split('?')
        return parts[1] if len(parts) > 1 else ''

    def dictToDate(d):
        if 'yg' in d.keys() and 'mg' in d.keys() and 'dg' in d.keys():
            return d['yg'] + '-' + d['mg'].zfill(2) + '-' + d['dg'].zfill(2)
        elif 'yg' in d.keys():
            return d['yg']
        return ''

    def browse(self, url):
        #print('    ', url)
        response = requests.get(url)
        parts = response.text.split('<h3')
        soup = BeautifulSoup(parts[0], "html.parser")
        people = People()
        people.sex = soup.select('h1 img')[0]['alt'].strip() if len(soup.select('h1 img')) > 0 else ''
        people.firstname = soup.select('h1 a')[0].text.strip() if len(soup.select('h1 a')) > 0 else ''
        people.lastname = soup.select('h1 a')[1].text.strip() if len(soup.select('h1 a')) > 1 else ''
        permalink_ = soup.select('h1 input')[0]['value'].strip() if len(soup.select('h1 input')) > 0 else ''
        parts = permalink_.replace('[', '').replace(']', '').split('/')
        people.permalink = ('p=%s;n=%s;' % (parts[0], parts[1]) + ('oc=%s' % (parts[2],) if parts[2] !='0' else '')) if len(parts) > 2 else ''
        dict1 = Process.extractParams(soup.select('ul li a.date')[0]['href'].strip()) if len(soup.select('ul li a.date')) > 0 else {}
        people.birthdate = Process.dictToDate(dict1)
        dict2 = Process.extractParams(soup.select('ul li a.date')[1]['href'].strip()) if len(soup.select('ul li a.date')) > 1 else {}
        people.deathdate = Process.dictToDate(dict2)
        people.birthplace = soup.select('ul li script')[0].text.strip().split('"')[1] if len(soup.select('ul li script')) > 0 else ''
        people.deathplace = soup.select('ul li script')[0].text.strip().split('"')[1] if len(soup.select('ul li script')) > 0 else ''
        print(people)
        people.save(DB)
        self.cache[url] = True
        DB.con.commit()
        soup = BeautifulSoup(response.text, "html.parser")
        parents = soup.find('h3', text='Parents')
        if parents:
            ul = parents.findNext('ul')
            links = ul.findAll('li')
            father_ = Process.extractQuery(links[0].find('a')['href'].strip()) if len(links) > 0 else ''
            father = self.browse(Process.base + father_) if father_ and Process.base + father_ not in self.cache.keys() else People()
            mother_ = Process.extractQuery(links[1].find('a')['href'].strip()) if len(links) > 1 else ''
            mother = self.browse(Process.base + mother_) if mother_ and Process.base + mother_ not in self.cache.keys() else People()
            if father.permalink or mother.permalink:
                family_id = father.permalink + '#' + mother.permalink
                DB.cur.execute('INSERT INTO family (id, father_permalink, mother_permalink) VALUES (?, ?, ?)', (family_id, father.permalink, mother.permalink))
                DB.cur.execute('UPDATE people SET family_id = ? WHERE permalink = ?', (family_id, people.permalink))
        spouses = soup.find('h3', text='Spouses and children')
        if spouses:
            ul = spouses.findNext('ul')
            links = ul.findAll('b')
            spouse_ = Process.extractQuery(links[0].find('a')['href'].strip()) if len(links) > 0 else ''
            if spouse_ and Process.base + spouse_ not in self.cache.keys():
                spouse = self.browse(Process.base + spouse_)
                dict1 = Process.extractParams(ul.select('li a.date')[0]['href'].strip()) if len(ul.select('li a.date')) > 0 else {}
                wedding_date = Process.dictToDate(dict1)
                wedding_place = ul.select('li script')[0].text.strip().split('"')[1] if len(ul.select('li script')) > 0 else ''
                family_id = (people.permalink + '#' + spouse.permalink) if people.sex == 'M' else (spouse.permalink + '#' + people.permalink)
                DB.cur.execute('UPDATE family SET wedding_date = ?, wedding_place = ? WHERE id = ?', (wedding_date, wedding_place, family_id))
        return people

if __name__ == '__main__':
    DB.update()
    process = Process('cache.json')
    process.init_caches()
    url = sys.argv[1] if len(sys.argv) > 1 else ''
    if (url):
        process.browse(url)
        DB.con.commit()
        process.save_caches()
    else:
        print('Please provide a URL')
