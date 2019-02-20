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


class Family:
    instances = {}

    def __init__(self, father_permalink, mother_permalink):
        self.id = father_permalink + '#' + mother_permalink
        self.father_permalink = father_permalink
        self.mother_permalink = mother_permalink
        self.wedding_date = self.wedding_place = ''
        Family.instances[self.id] = self

    def get(father_permalink, mother_permalink):
        family_id = father_permalink + '#' + mother_permalink
        return Family.instances[family_id] if family_id in Family.instances.keys() else Family(father_permalink, mother_permalink)

    def save(self):
        DB.cur.execute('INSERT INTO family (id, father_permalink, mother_permalink, wedding_date, wedding_place) VALUES (?, ?, ?, ?, ?)', (self.id, self.father_permalink, self.mother_permalink, self.wedding_date, self.wedding_place))

class People:
    def __init__(self, permalink, firstname = '', lastname = '', sex = '', birthdate = '', birthplace = '', deathdate = '', deathplace = '', family_id = ''):
        self.permalink = permalink
        self.firstname = firstname
        self.lastname = lastname
        self.sex = sex
        self.birthdate = birthdate
        self.birthplace = birthplace
        self.deathdate = deathdate
        self.deathplace = deathplace
        self.permalink = permalink
        self.family_id = family_id

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
                cache = json.load(f)
                DB.cur.execute('SELECT firstname, lastname, sex, birthdate, birthplace, deathdate, deathplace, permalink, family_id FROM people')
                for (firstname, lastname, sex, birthdate, birthplace, deathdate, deathplace, permalink, family_id) in DB.cur.fetchall():
                    people = People(permalink, firstname, lastname, sex, birthdate, birthplace, deathdate, deathplace, family_id)
                    for (k, v) in cache.items():
                        if v == people.permalink:
                            self.cache[k] = people
                            continue

    def save_caches(self):
        with open(self.filename, 'w') as f:
            for (k, v) in self.cache.items():
                self.cache[k] = v.permalink
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

    def browse(self, path):
        response = requests.get(Process.base + path)
        parts = response.text.split('<h3')
        soup = BeautifulSoup(parts[0], "html.parser")
        permalink_ = soup.select('h1 input')[0]['value'].strip() if len(soup.select('h1 input')) > 0 else ''
        parts = permalink_.replace('[', '').replace(']', '').split('/')
        permalink = ('p=%s;n=%s;' % (parts[0], parts[1]) + ('oc=%s' % (parts[2],) if parts[2] !='0' else '')) if len(parts) > 2 else ''
        people = People(permalink)
        people.sex = soup.select('h1 img')[0]['alt'].strip() if len(soup.select('h1 img')) > 0 else ''
        people.firstname = soup.select('h1 a')[0].text.strip() if len(soup.select('h1 a')) > 0 else ''
        people.lastname = soup.select('h1 a')[1].text.strip() if len(soup.select('h1 a')) > 1 else ''
        dict1 = Process.extractParams(soup.select('ul li a.date')[0]['href'].strip()) if len(soup.select('ul li a.date')) > 0 else {}
        people.birthdate = Process.dictToDate(dict1)
        dict2 = Process.extractParams(soup.select('ul li a.date')[1]['href'].strip()) if len(soup.select('ul li a.date')) > 1 else {}
        people.deathdate = Process.dictToDate(dict2)
        people.birthplace = soup.select('ul li script')[0].text.strip().split('"')[1] if len(soup.select('ul li script')) > 0 else ''
        people.deathplace = soup.select('ul li script')[1].text.strip().split('"')[1] if len(soup.select('ul li script')) > 1 else ''
        print(people)
        people.save(DB)
        self.cache[path] = people
        DB.con.commit()
        soup = BeautifulSoup(response.text, "html.parser")
        parents = soup.find('h3', text='Parents')
        if parents:
            ul = parents.findNext('ul')
            links = ul.findAll('li')
            father_ = Process.extractQuery(links[0].find('a')['href'].strip()) if len(links) > 0 else ''
            if father_:
                father = self.cache[father_] if father_ in self.cache.keys() else self.browse(father_)
            father_permalink = father.permalink if father else ''
            mother_ = Process.extractQuery(links[1].find('a')['href'].strip()) if len(links) > 1 else ''
            if mother_:
                mother = self.cache[mother_] if mother_ in self.cache.keys() else self.browse(mother_)
            mother_permalink = mother.permalink if mother else ''
            if father_permalink or mother_permalink:
                family = Family.get(father_permalink, mother_permalink)
                family.save()
                DB.cur.execute('UPDATE people SET family_id = ? WHERE permalink = ?', (family.id, people.permalink))
        spouses = soup.find('h3', text='Spouses and children')
        if spouses:
            ul = spouses.findNext('ul')
            links = ul.findAll('b')
            spouse_ = Process.extractQuery(links[0].find('a')['href'].strip()) if len(links) > 0 else ''
            if spouse_ and spouse_ not in self.cache.keys():
                spouse = self.browse(spouse_)
                dict1 = Process.extractParams(ul.select('li a.date')[0]['href'].strip()) if len(ul.select('li a.date')) > 0 else {}
                wedding_date = Process.dictToDate(dict1)
                wedding_place = ul.select('li script')[0].text.strip().split('"')[1] if len(ul.select('li script')) > 0 else ''
                father_permalink = people.permalink if people.sex == 'M' else spouse.permalink
                mother_permalink = spouse.permalink if people.sex == 'M' else people.permalink
                family = Family.get(father_permalink, mother_permalink)
                family.wedding_date = wedding_date
                family.wedding_place = wedding_place
                family.save()
                print('W %s %s %s' % (wedding_date, wedding_place, family.id))
        return people

    def export(self, filename):
        with open(filename, 'w') as f:
            DB.cur.execute('SELECT firstname, lastname, sex, birthdate, birthplace, deathdate, deathplace, permalink, family_id FROM people')
            f.write('person,grampsid,firstname,lastname,gender,note,birthdate,birthplace,deathdate,deathplace\n')
            people = DB.cur.fetchall()
            for (firstname, lastname, sex, birthdate, birthplace, deathdate, deathplace, permalink, family_id) in people:
                sex = 'male' if sex == 'M' else 'female' if sex == 'F' else ''
                source = Process.base + permalink
                f.write('%s,,"%s","%s",%s,%s,%s,"%s",%s,"%s"\n' % (permalink, firstname, lastname, sex, source, birthdate, birthplace, deathdate, deathplace))
            f.write('\n\nmarriage,husband,wife,date,place,source\n')
            DB.cur.execute('SELECT id, father_permalink, mother_permalink, wedding_date, wedding_place FROM family')
            for (family_id, father_permalink, mother_permalink, wedding_date, wedding_place) in DB.cur.fetchall():
                f.write('%s,%s,%s,%s,"%s"\n' % (family_id, father_permalink, mother_permalink, wedding_date or '', wedding_place or ''))
            f.write('\n\nfamily,child\n')
            for (firstname, lastname, sex, birthdate, birthplace, deathdate, deathplace, permalink, family_id) in people:
                f.write('%s,%s\n' % (family_id or '', permalink))

if __name__ == '__main__':
    DB.update()
    process = Process('cache.json')
    process.init_caches()
    url = sys.argv[1] if len(sys.argv) > 1 else ''
    if (url):
        process.browse(url.replace(Process.base, ''))
        DB.con.commit()
        process.save_caches()
        process.export('export.csv')
    else:
        print('Please provide a URL')
