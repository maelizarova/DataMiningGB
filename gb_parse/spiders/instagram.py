import datetime as dt
import json
import scrapy
import requests
from pymongo import MongoClient

class InstagramSpider(scrapy.Spider):
    name = 'instagram'
    allowed_domains = ['www.instagram.com']
    start_urls = ['https://www.instagram.com/']
    login_url = 'https://www.instagram.com/accounts/login/ajax/'
    api_url = '/graphql/query/'
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.0; rv:70.0) Gecko/20100101 Firefox/70.0", }
    params = {}

    query_hash = {
        # 'posts': '56a7068fea504063273cc2120ffd54f3',
        # 'tag_posts': "9b498c08113f1e09617a1703c22b2f32",
        'recommend_friends': "ed2e3ff5ae8b96717476b62ef06ed8cc&variables",
        'user': "d4d88dc1500312af6f937f7b804c68c3",
        'subscribers': "c76146de99bb02f6415203be841dd25a&variables",
        'subscription': "d04b0a864b4b54837c0d870b0e77e076&variables",
    }

    def __init__(self, login, enc_password,  *args, **kwargs):
        self.login = login
        self.enc_passwd = enc_password
        self.db = MongoClient()['parse_13']

        super().__init__(*args, **kwargs)

    def parse(self, response, **kwargs):
        try:
            js_data = self.js_data_extract(response)
            yield scrapy.FormRequest(
                self.login_url,
                method='POST',
                callback=self.parse,
                formdata={
                    'username': self.login,
                    'enc_password': self.enc_passwd,
                },
                headers={'X-CSRFToken': js_data['config']['csrf_token']}
            )
        except AttributeError as e:
            if response.json().get('authenticated'):
                variables = {
                    'fetch_media_count': 0,
                    'fetch_suggested_count': 30,
                    'ignore_cache': True,
                    'filter_followed_friends': True,
                    'seen_ids': [],
                    'include_reel': True,

                }

                yield response.follow(
                    url=f'{self.api_url}?query_hash={self.query_hash["recommend_friends"]}&variables={json.dumps(variables)}',
                    callback=self.recommened_parse
                )

    def recommened_parse(self, response):
        variables = {
            'user_id': "",
            'include_chaining': True,
            'include_reel': True,
            'include_suggested_users': False,
            'include_logged_out_extras': False,
            'include_highlight_reels': False,
            'include_live_status': True,

        }
        rf_list = json.loads(response.text)['data']['user']['edge_suggested_users']['edges']
        for rf in rf_list:
            u_id = rf['node']['user']['id']
            variables['user_id'] = u_id

            yield response.follow(
                url=f'{self.api_url}?query_hash={self.query_hash["user"]}&variables={json.dumps(variables)}',
                callback=self.user_parse
            )

    def user_parse(self, response):
        subscribers_variables = {
            'id': "",
            'include_reel': True,
            'fetch_mutual': True,
            'first': 1,
        }
        subscription_variables = {
            'id': "",
            'include_reel': True,
            'fetch_mutual': True,
            'first': 1,
        }

        user_common_date = {}

        user_data = json.loads(response.text)

        user_common_date['id'] = user_data['data']['user']['reel']['user']['id']
        user_common_date['username'] = user_data['data']['user']['reel']['user']['username']


        subscribers_variables['id'] = user_common_date['id']
        response_1 = requests.get(
            f'https://www.instagram.com{self.api_url}?query_hash={self.query_hash["subscribers"]}&variables={json.dumps(subscribers_variables)}',
            params=self.params,
            headers=self.headers
        )
        slist_data = json.loads(response_1.text)
        subscribers_variables['first'] = slist_data['data']['user']['edge_followed_by']['count']

        user_common_date['subscribers'] = self.subscribers_parse(requests.get(
            f'https://www.instagram.com{self.api_url}?query_hash={self.query_hash["subscribers"]}&variables={json.dumps(subscribers_variables)}',
            params=self.params,
            headers=self.headers
        ))

        # yield response.follow(
        #     url=f'{self.api_url}?query_hash={self.query_hash["subscribers"]}&variables={json.dumps(subscribers_variables)}',
        #     callback=self.subscribers_parse
        # )

        subscription_variables['id'] = user_data['data']['user']['reel']['id']
        response_2 = requests.get(
            f'https://www.instagram.com{self.api_url}?query_hash={self.query_hash["subscription"]}&variables={json.dumps(subscription_variables)}',
            params=self.params,
            headers=self.headers
        )
        slist_data = json.loads(response_2.text)
        # print(slist_data['data']['user']['edge_followed_by']['count'])
        subscription_variables['first'] = slist_data['data']['user']['edge_follow']['count']

        user_common_date['subscription'] = self.subscription_parse(requests.get(
            f'https://www.instagram.com{self.api_url}?query_hash={self.query_hash["subscription"]}&variables={json.dumps(subscription_variables)}',
            params=self.params,
            headers=self.headers
        ))
        # yield response.follow(
        #     url=f'{self.api_url}?query_hash={self.query_hash["subscription"]}&variables={json.dumps(subscription_variables)}',
        #     callback=self.subscription_parse
        # )
        collection = self.db['Instagramm']
        collection.insert_one(user_common_date)

    def subscribers_parse(self, response): # список подпсчиков
        result = []
        user_data = json.loads(response.text)
        for el in user_data['data']['user']['edge_followed_by']['edges']:
            result.append({'id': el['node']['id'], 'username': el['node']['username'], 'full_name': el['node']['full_name']})

        return result

    def subscription_parse(self, response): # список подпсок
        result = []
        user_data = json.loads(response.text)
        for el in user_data['data']['user']['edge_follow']['edges']:
            result.append({'id': el['node']['id'], 'username': el['node']['username'], 'full_name': el['node']['full_name']})

        return result

    @staticmethod
    def js_data_extract(response):
        script = response.xpath('//script[contains(text(), "window._sharedData =")]/text()').get()
        return json.loads(script.replace("window._sharedData =", '')[:-1])
