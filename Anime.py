import os
import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import time

directory = r'D:/Prog/GitHub/Anime/'

if not os.path.exists(directory):
    os.makedirs(directory)

db_filename = 'anime_data_new.db'
db_file = os.path.join(directory, db_filename)

conn = sqlite3.connect(db_file)
cursor = conn.cursor()

try:
    cursor.execute('''CREATE TABLE IF NOT EXISTS anime (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        type TEXT,
                        episodes INTEGER,
                        start_date TEXT,
                        end_date TEXT,
                        members TEXT
                    )''')

    print("Database and table created successfully.")

except sqlite3.Error as e:
    print("Error occurred while creating table:", e)

def parse_and_insert_data(soup):
    anime_info = soup.find_all('div', class_='information di-ib mt4')

    anime_info = [info for info in anime_info if 'eps' in info.text]

    for info in anime_info:
        name_tag = info.find_previous_sibling('div', class_='di-ib clearfix')
        if name_tag:
            name_text = name_tag.a.text.strip()
        else:
            name_text = "N/A"

        info_text = info.text.strip()

        match = re.search(r'(\w+) \((\d+) eps?\)', info_text)
        if match:
            type_text, episodes = match.groups()
        else:
            type_text = episodes = None

        match = re.search(r'(\w{3} \d{4}) - (\w{3} \d{4})', info_text)
        if match:
            start_date, end_date = match.groups()
        else:
            start_date = end_date = None

        match = re.search(r'(\d[\d,]+) members', info_text)
        members = match.group(1) if match else None

        print("Data to insert:", name_text, type_text, episodes, start_date, end_date, members)
        try:
            cursor.execute('''INSERT INTO anime (name, type, episodes, start_date, end_date, members) VALUES (?, ?, ?, ?, ?, ?)''', (name_text, type_text, episodes, start_date, end_date, members))
            print("Data inserted successfully for:", name_text)
        except sqlite3.Error as e:
            print("Error occurred while inserting data:", e)

    try:
        conn.commit()
        print("Data committed successfully to the database.")
    except sqlite3.Error as e:
        print("Error occurred while committing data:", e)

def parse_next_pages(base_url):
    parsed_pages = 0
    offset = 0
    next_url = base_url

    while True:
        response = requests.get(next_url, params={'limit': 50, 'offset': offset})
        soup = BeautifulSoup(response.text, 'html.parser')

        parse_and_insert_data(soup)

        parsed_pages += 1
        print(f"Parsed page {parsed_pages} - {next_url}")

        offset += 50

        next_page = soup.find('a', class_='link-blue-box next')
        if next_page:
            next_url = 'https://myanimelist.net/topanime.php' + next_page['href'] + f"&limit=50&offset={offset}"
        else:
            break

        time.sleep(1)


base_url = 'https://myanimelist.net/topanime.php'

try:
    parse_next_pages(base_url)

except KeyboardInterrupt:
    print("Программа прервана пользователем.")
    conn.close()

conn.close()

print("Данные успешно спарсены и добавлены в базу данных в директории:", directory)
