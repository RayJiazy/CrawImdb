import os.path
from compress import *
import requests
from bs4 import BeautifulSoup
import logging
import csv
from tqdm import tqdm
import json
import pandas as pd
import mysql.connector

db = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Jzy969811",
  database="movie_sys"
)
cursor = db.cursor()

class Model():
    def __init__(self):
        # 请求头
        self.headers = {
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        }
        # 存放每一步电影的id和imdb的id
        self.movie_dct = []
        # 存放已经处理完的movie id
        self.white_lst = []
        # 电影详情的初始url
        self.url = 'https://www.imdb.com/title/'
        self.movie_csv_path = 'info/movie_info.csv'
        self.extra_info_path = "info/extra_info.csv"
        self.uns_path = 'log/unsuccesful.csv'
        self.white_path = "log/white_list"
        self.black_path = "log/black_list"
        # 海报的保存路径
        self.poster_save_path = './poster'
        # logging的配置，记录运行日志
        logging.basicConfig(filename="log/run.log", filemode="a+", format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO)
        # 表示当前处理的电影
        self.cur_imdb_id = None

    def get_white_lst(self):
        '''获取处理完的白名单'''
        if os.path.isfile(self.white_path):
            with open(self.white_path) as fb:
                for line in fb:
                    line = line.strip()
                    self.white_lst.append(line)

    def get_movie_id(self):
        '''获取电影的id和imdb的id'''
        self.movie_dct = pd.read_csv(self.movie_csv_path)[["imdbId"]].values

    def update_white_lst(self, movie_id):
        '''更新白名单'''
        with open(self.white_path, 'a+') as fb:
            fb.write(str(movie_id) + '\n')

    def update_black_lst(self, movie_id, msg=''):
        with open(self.black_path, 'a+') as fb:
            # 写入movie id 和imdb id，并且加上错误原因
            # msg=1是URL失效，msg=2是电影没有海报
            fb.write(str(movie_id) + ' ' + msg + '\n')

    def get_url_response(self, url):
        '''访问网页请求，返回response'''
        logging.info(f'get {url}')
        i = 0
        # 超时重传，最多5次
        while i < 5:
            try:
                response = requests.get(url, headers=self.headers, timeout=6)
                if response.status_code == 200:
                    logging.info(f'get {url} sucess')
                    # 正常获取，直接返回
                    return response
                # 如果状态码不对，获取失败，返回None，不再尝试
                logging.error(f'get {url} status_code error: {response.status_code} movie_id is {self.cur_imdb_id}')
                return None
            except requests.RequestException:
                # 如果超时
                logging.error(f'get {url} error, try to restart {i + 1}')
                i += 1
        # 重试5次都失败，返回None
        return None

    def process_html(self, html):
        '''解析html，获取海报，电影信息'''
        soup = BeautifulSoup(html, 'lxml')
        script = soup.find_all('script')[2]
        script_content = script.string
        data = json.loads(script_content)
        image_url = data.get("image",None)
        director = data["director"][0]["name"] if "director" in data else None
        response = self.get_url_response(image_url)
        if response != None:
            response.raise_for_status()
            with open(f"poster/{self.cur_imdb_id}.jpeg", "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            compress_image(f"poster/{self.cur_imdb_id}.jpeg",f"poster/{self.cur_imdb_id}.jpeg")
            return [[int(self.cur_imdb_id),director,f"poster/{self.cur_imdb_id}.jpeg"]], 1
        else:
            # 如果没有海报链接，那么在黑名单中更新它
            # msg=2表示没有海报链接
            logging.error(f'get {image_url} error, try to restart {self.cur_imdb_id}')
            self.update_black_lst(self.cur_imdb_id, '2')
            return [], 0

    def save_info(self, detail):
        # 存储到CSV文件中
        with open(f'{self.uns_path}', 'a+', encoding='utf-8', newline='') as fb:
            writer = csv.writer(fb)
            writer.writerow(detail)

    def run(self):
        self.get_white_lst()
        self.get_movie_id()
        for imdb_id in tqdm(self.movie_dct):
            if str(imdb_id[0]) in self.white_lst:
                continue
            self.cur_imdb_id = imdb_id[0]
            response = self.get_url_response(self.url + 'tt' + str(self.cur_imdb_id).zfill(7))
            # 找不到电影详情页的url，或者超时，则仅仅保留id，之后再用另一个脚本处理
            if response == None:
                self.save_info([self.cur_imdb_id, '' * 9])
                # 仍然更新白名单，避免重复爬取这些失败的电影
                self.update_white_lst(self.cur_imdb_id)
                # 更新黑名单，爬完之后用另一个脚本再处理
                self.update_black_lst(self.cur_imdb_id, '1')
                continue
            # 处理电影详情信息
            res, flag = self.process_html(response.content)
            if flag:
                res = pd.DataFrame(res,columns=['imdbId', 'director', 'poster'])
                res.to_csv(self.extra_info_path,mode="a",header= False, index=False)
            else:
                continue
            # 处理完成，增加movie id到白名单中
            self.update_white_lst(self.cur_imdb_id)
            logging.info(f'process movie {self.cur_imdb_id} success')

if __name__ == '__main__':
    s = Model()
    s.run()
