#!/usr/bin/env python
import requests, json, time, argparse, os, sys, sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, ALL_COMPLETED
from datetime import datetime

conn = sqlite3.connect('bsc.db')
conn.row_factory = sqlite3.Row

song_folder = "songs/"
dl_batchsize = 8
fetch_timeout = 2
dl_timeout = 5


session_total = 0
ap = argparse.ArgumentParser(
    description='Automated Beatsaver downloader')
ap.add_argument('--fetch',
                action='store_true',
                required=False,
                help='[Default] Only fetch, no download')
ap.add_argument('--download',
                action='store_true',
                required=False,
                help='Actually download songs')
ap.add_argument('--song_dir',
                required=False,
                type=str,
                help='Put songs here (default: pwd/songs')

args = ap.parse_args()

#Use FF Headers. Otherwise BS will return a 403
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-language': 'en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7,ja-JP;q=0.6,ja;q=0.5,en-GB;q=0.4',
    'cache-control': 'max-age=0',
    'dnt': '1',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'upgrade-insecure-requests': '1'
}

def init_db():
    conn.execute('''CREATE TABLE IF NOT EXISTS SONGS
         (id                INTEGER PRIMARY KEY,
         SongName           CHAR(255)   NOT NULL,
         Key                CHAR(25)    NOT NULL,
         AuthorName         CHAR(255)   NOT NULL,
         DirectDownload     CHAR(255)   NOT NULL,
         TimeAdded          DATETIME DEFAULT CURRENT_TIMESTAMP,
         State              INT);''')
    conn.execute('''CREATE TABLE IF NOT EXISTS CRAWLER
         (id                INTEGER PRIMARY KEY,
         InitialPageCount       INT,
         LastFetched            CHAR(25),
         LastDownloaded         CHAR(25));''')
    print("DB Initialized");

#sanitize-filename being used by beatsaver (https://github.com/parshap/node-sanitize-filename)
def sanitize(string):
    b = '/?<>\\:*|"'
    for char in b:
        string = string.replace(char, "")
    return string

def download_song(link, filename, song_data):
    result = 1
    error = 'none'
    filepath = "{folder}{filename}".format(folder=song_folder, filename=filename)
    try:
        with open(filepath, "wb") as f:
            now = datetime.now()
            print("[{dt}] Downloading {name}".format(dt=now.strftime("%H:%M:%S"), name=filename))
            response = requests.get("https://beatsaver.com{}".format(link), stream=False, allow_redirects=True, headers=headers)
            total_length = response.headers.get('content-length')

            if total_length is None: 
                result = 1 #If teh content legth header is not avaialable, something's wrong
                error = 'Missing content length header - HTTP {}'.format(response.status_code)
            else:
                dl = 0
                total_length = int(total_length)
                for data in response.iter_content(chunk_size=4096):
                    dl += len(data)
                    f.write(data)
                    done = int(50 * dl / total_length)
                   
                    percent_done = float(dl)/total_length*100
                    print("[{fill}{space}]{percent:.1f}% ({mbDone:.2f}/{mbTotal:.2f}MB)\r".format(
                        fill='=' * done, 
                        space=' ' * (50-done), 
                        percent=percent_done, 
                        mbDone=int(dl) / float(1 << 20), 
                        mbTotal=int(total_length) / float(1 << 20) ), end="\r")    
                if dl != total_length:
                    result = 1
                    error = 'Download size did not match expected content length'
                else:
                    result = 0
            print("")
    except requests.exceptions.RequestException as e:
        result = 1
    return { 'result': result, 'link': link, 'filename': filename, 'songData': song_data, 'error': error }

def download_last_n(n):
    global session_total
    threads = []
    executor = ThreadPoolExecutor(max_workers=n)

    dl_queue = conn.execute("SELECT id,SongName,Key,AuthorName,DirectDownload,State from SONGS WHERE State = 0 OR State = 1 ORDER BY id DESC LIMIT ?", (n,)).fetchall()
    for song in dl_queue:
        filename = "{key} ({songName} - {mapper}).zip".format(key=song['Key'], songName=song['SongName'], mapper=song['AuthorName'])
        filename = sanitize(filename)
        threads.append(executor.submit(download_song, song['DirectDownload'], filename, song ))
        
    for task in as_completed(threads):
        #dl_res = download_song(song['DirectDownload'], filename)
        song_data = task.result()['songData']
        if task.result()['result'] == 0:
            #success
            #conn.execute("UPDATE SONGS set State = ? where ID = ?", (2,song['id'],))
            conn.execute("UPDATE SONGS set State = ? where ID = ?", (2,song_data['id'],))
            session_total += 1
            print("Successfully downloaded {} - {}".format(song_data['Key'], song_data['SongName']))
            conn.commit()
        else:
            #failed
            conn.execute("UPDATE SONGS set State = ? where ID = ?", (1,song_data['id'],))
            print("Failed to download {} - {}. Reason: {}".format(song_data['Key'],song_data['SongName'], task.result()['error']))
            conn.commit()
    print("Batch of {n} completed (Session total: {st}".format(n=n,st=session_total))


def insert_if_missing(songdata):
    key = songdata['key']
    song_name = songdata['metadata']['songName']
    author_name = songdata['metadata']['levelAuthorName']
    direct_download = songdata['directDownload']
    db_data = conn.execute("SELECT id from SONGS where key=?",(key,)).fetchone()

    if db_data:
        print('{} exists'.format(key))
    else:
        print('Adding new song {}'.format(songdata['metadata']['songName']))
        conn.execute("INSERT INTO SONGS (id,SongName,Key,AuthorName,DirectDownload,State) \
        VALUES ( NULL, ?, ?, ?, ?, 0)", (song_name, key, author_name, direct_download,));

        conn.commit()

def add_to_db(data):
    for i in range(len(data['docs'])):
        insert_if_missing(data['docs'][i])

def fetch_latest(page):
    try:
        response = requests.get('https://beatsaver.com/api/maps/latest/{}'.format(page), headers=headers)
        with open('response.html', 'wb') as f:
            f.write(response.content)
        data = json.loads(response.text)
    except requests.exceptions.RequestException as e:
        print("Failed to fetch page {} retrying in 10s".format(page))
        data = fetch_latest(page)
    return data

def index(page):
    print("Indexing page {}".format(page))
    data = fetch_latest(page)
    add_to_db(data)
    conn.execute("UPDATE CRAWLER set LastFetched = ? where ID = 1", (page,))
    conn.commit()

    state = conn.execute("SELECT InitialPageCount, LastFetched, LastDownloaded from CRAWLER").fetchone()
    page_count = data['lastPage']
    new_next_page = int(state['LastFetched']) + (int(page_count) - int(state['InitialPageCount']))

    print("API count: {} - DB Count: {}".format(int(page_count), state['InitialPageCount']))
    if new_next_page != page:
        print("Page offset changed! Now indexing from {}".format(new_next_page))
        conn.execute("UPDATE CRAWLER set InitialPageCount = ? where ID = 1", (int(page_count),))
    return new_next_page - 1

def start():

    if not os.path.exists(song_folder):
        os.makedirs(song_folder)
    
    if args.download:
        #Download mode
        print("Download mode")
        while True:
            left = conn.execute("SELECT COUNT(*) from SONGS WHERE State = 0 OR State = 1").fetchone()[0]
            batch_size = min(dl_batchsize, left)
            if batch_size <= 0:
                print("No more songs queued")
                break
            print("{left} songs left to download. Downloading batch of {batchSize}".format(left=left, batchSize=batch_size))
            download_last_n(batch_size)
            print("Waiting {}s".format(dl_timeout))
            time.sleep(dl_timeout)
    else:
        #Fetch mode
        print("Fetch mode")
        next_page = None
        state = conn.execute("SELECT InitialPageCount, LastFetched, LastDownloaded from CRAWLER").fetchone()
        
        print("Inititalizing crawler...")

        data = fetch_latest(0)
        page_count = data['lastPage']
        
        if not state:
            print('First run')
            conn.execute("INSERT INTO CRAWLER (id, InitialPageCount) \
            VALUES (NULL, '{pageCount}')".format(\
            pageCount=page_count));
            conn.commit()
            next_page = page_count
        else:
            #Np = Lf + (Pc - Pi)
            next_page = int(state['LastFetched']) + (int(page_count) - int(state['InitialPageCount']))
        
        print('Indexing from page {}'.format(next_page))

        while next_page > -1:
            next_page = index(next_page)
            print('Waiting {} s'.format(fetch_timeout))
            time.sleep(fetch_timeout)
        
print("Automated Beatsaver downloader")
init_db()
start()