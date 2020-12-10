import time
import datetime as dt
import requests
import bs4
import pymongo
from urllib.parse import urljoin


class MagnitParse:
    headers = { 
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.0; rv:70.0) Gecko/20100101 Firefox/70.0'
    }
    
    def __init__(self, start_url):
        self.start_url = start_url
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client['parse_GB']

        self.months_template = {
            'января': 1,
            'февраля': 2,
            'марта': 3,
            'апреля': 4,
            'мая': 5,
            'июня': 6,
            'июля': 7,
            'августа': 8,
            'сентября': 9,
            'октября': 10,
            'ноября': 11,
            'декабря': 12
        }

    @staticmethod
    def _get(*args, **kwargs):
        while True:
            try:
                response = requests.get(*args, **kwargs)
                if response.status_code != 200:
                    raise Exception
                return response
            except Exception:
                time.sleep(0.5)

    def soup(self, url) -> bs4.BeautifulSoup:
        response = self._get(url, headers=self.headers)
        return bs4.BeautifulSoup(response.text, 'lxml')

    def run(self):
        soup = self.soup(self.start_url)
        for product in self.parse(soup):
            self.save(product)

    def parse_date(self, str_):
        str_ = str_.split(' ')[1:]
        return dt.datetime(year=dt.datetime.now().year, day=int(str_[0]),
                           month=self.months_template[str_[1]])

    def parse(self, soup):
        catalog = soup.find('div', attrs={'class': 'сatalogue__main'})

        for product in catalog.find_all('a', recursive=False):
            pr_data = self.get_product(product)
            yield pr_data

    def get_product(self, product_soup) -> dict:
        dates = product_soup.find('div', attrs={'class': 'card-sale__date'}).text.strip('\n').split('\n')
        product_template = {
            "url": lambda soup: urljoin(self.start_url, soup.get('href')),
            "promo_name": lambda soup: str(soup.find('div', attrs={'class': 'card-sale__header'}).text),
            "product_name": lambda soup: str(soup.find('div', attrs={'class': 'card-sale__title'}).text),
            "old_price": lambda soup: float(soup.find('div', attrs={'class': 'label__price_old'}).text.strip('\n').replace('\n', '.')),
            "new_price": lambda soup: float(soup.find('div', attrs={'class': 'label__price_new'}).text.strip('\n').replace('\n', '.')),
            "image_url": lambda soup: str(urljoin(self.start_url, soup.find('img').get('data-src'))),
            "date_from": lambda _: self.parse_date(dates[0]),
            "date_to": lambda _: self.parse_date(dates[1])
        }
        result = {}
        for key, value in product_template.items():
            try:
                result[key] = value(product_soup)
            except Exception as e:
                continue
        print(1)
        return result

    def save(self, product):
        collection = self.db['parse_magnit']
        collection.insert_one(product)


if __name__ == '__main__':
    parser = MagnitParse('https://magnit.ru/promo/?geo=moskva')
    parser.run()