import os
import time
import requests
import bs4
import pymongo
import dotenv
from urllib.parse import urljoin

dotenv.load_dotenv('.env')


class CianParse:
    _headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'}

    def __init__(self, start_url: str):
        self.start_url = start_url
        self.page_done = set()
        client = pymongo.MongoClient(os.getenv('DATA_BASE'))
        self.db = client['moscow']

    def _get(self, url: str) -> bs4.BeautifulSoup:
        while True:
            try:
                response = requests.get(url, headers=self._headers)
                if response.status_code != 200:
                    raise Exception
                self.page_done.add(url)
                soup = bs4.BeautifulSoup(response.text, 'lxml')
                return soup
            except Exception:
                time.sleep(1)

    def run(self, url=None):
        if not url:
            url = self.start_url

        if url not in self.page_done:
            soup = self._get(url)
            posts, pagination = self._parse(soup)

            for post in posts:
                page_data = self._page_parse(post)
                self.save(page_data)

            for pag_url in pagination:
                time.sleep(0.5)
                self.run(pag_url)

    @staticmethod
    def _parse(soup):
        pag_div = soup.find('div', attrs={'data-name': 'Pagination'})
        pagination = set(urljoin('https://www.cian.ru/', pag.get('href'))
                         for pag in pag_div.findAll('a') if pag.get('href'))
        posts = set(post.find('a').get('href') for post in soup.findAll('div', attrs={'data-name': 'LinkArea'}))
        return posts, pagination

    def _page_parse(self, url):
        soup = self._get(url)
        print(1)

        try:
            geo_data = soup.find('div', attrs={'data-name': 'Geo'}).span.get('content').split(',')
        except AttributeError:
            geo_data = None

        try:
            n_rooms = int(soup.find('div', attrs={'data-name': 'OfferTitle'}).h1.text[0])
        except (AttributeError, ValueError):
            n_rooms = ''

        summary_description = {
            s_data.findAll('div')[1].text: s_data.findAll('div')[0].text.split()
            for s_data in soup.findAll('div', attrs={'data-testid': 'object-summary-description-info'})}

        general_info = {li.findAll('span')[0].text: li.findAll('span')[1].text for li in
                        soup.findAll('li', attrs={'data-name': 'AdditionalFeatureItem'})}
        bathrooms = self._get_attrs_dict(general_info['Санузел']) if 'Санузел' in general_info.keys() else {}
        balconies = self._get_attrs_dict(
            general_info['Балкон/лоджия']) if 'Балкон/лоджия' in general_info.keys() else {}

        house_data = {h_data.findAll('div')[0].text: h_data.findAll('div')[1].text for h_data in
                      soup.findAll('div', attrs={'data-name': 'Item'})}
        lifts_data = self._get_attrs_dict(house_data['Лифты']) if 'Лифты' in general_info.keys() else {}

        transportation = [span.text.split() for span in
                          soup.findAll('span', attrs={'class': 'a10a3f92e9--underground_time--1fKft'})]
        trans_data = self._get_transportation_dict(url, transportation)

        response = {
            'n_rooms': n_rooms,
            'territorial_division': geo_data[1] if geo_data else '',
            'district': ' '.join(geo_data[2].split()[1:]) if geo_data else '',
            'total_area': float('.'.join(summary_description['Общая'][0].split(',')))
            if 'Общая' in summary_description.keys() else '',
            'living_space': float('.'.join(summary_description['Жилая'][0].split(',')))
            if 'Жилая' in summary_description.keys() else '',
            'kitchen_area': float('.'.join(summary_description['Кухня'][0].split(',')))
            if 'Кухня' in summary_description.keys() else '',
            'floor': int(summary_description['Этаж'][0]) if 'Этаж' in summary_description.keys() else '',
            'total_floors': int(summary_description['Этаж'][2]) if 'Этаж' in summary_description.keys() else '',
            'year_of_built': int(summary_description['Построен'][0]) if 'Построен'
                                                                        in summary_description.keys() else '',
            'celling_height': float('.'.join(general_info['Высота потолков'].split()[0].split(',')))
            if 'Высота потолков' in general_info.keys() else '',
            'renovation_type': general_info['Ремонт'] if 'Ремонт' in general_info.keys() else '',
            'outside_view': general_info['Вид из окон'] if 'Вид из окон' in general_info.keys() else '',
            'n_balconies': int(balconies['балк']) if 'балк' in bathrooms.keys() else 0,
            'n_loggias': int(balconies['лодж']) if 'лодж' in bathrooms.keys() else 0,
            'n_sep_bathrooms': int(bathrooms['разд']) if 'разд' in bathrooms.keys() else 0,
            'n_comb_bathrooms': int(bathrooms['совм']) if 'совм' in bathrooms.keys() else 0,
            'building_type': house_data['Тип дома'] if 'Тип дома' in house_data.keys() else '',
            'overlap_type': house_data['Тип перекрытий'] if 'Тип перекрытий' in house_data.keys() else '',
            'n_pass_lifts': int(lifts_data['пасс']) if 'пасс' in lifts_data.keys() else 0,
            'n_serv_lifts': int(lifts_data['груз']) if 'груз' in lifts_data.keys() else 0,
            'heating_type': house_data['Отопление'] if 'Отопление' in house_data.keys() else '',
            'home_emergency': house_data['Аварийность'] if 'Аварийность' in house_data.keys() else '',
            'parking_type': house_data['Парковка'] if 'Парковка' in house_data.keys() else '',
            'garbage_chute': house_data['Мусоропровод'] if 'Мусоропровод' in house_data.keys() else '',
            'mins_to_subway_by_walk': trans_data['пешк'],
            'mins_to_subway_by_car': trans_data['маши'],
            'mins_to_subway_by_trans': trans_data['тран'],
            'price': self._get_price(soup)
        }
        return response

    @staticmethod
    def _get_price(soup):
        try:
            raw_price = soup.find('div', attrs={'data-name': 'OfferTerms'}
                                  ).find('span', attrs={'itemprop': 'price'}).get('content')
            price = float(''.join(raw_price.split()[:-1]))
        except (ValueError, AttributeError):
            return 0.0
        else:
            return price

    @staticmethod
    def _get_transportation_dict(transportation: list) -> dict:
        current_dict = {
            'тран': [],
            'пешк': [],
            'маши': []
        }

        for t_data in transportation:
            if t_data[-1][:4].isalpha():
                try:
                    current_dict[t_data[-1][:4]].append(int(t_data[1]))
                except ValueError:
                    continue

        result = {'тран': min(current_dict['тран']) if current_dict['тран'] else '',
                  'пешк': min(current_dict['пешк']) if current_dict['пешк'] else '',
                  'маши': min(current_dict['маши']) if current_dict['маши'] else ''}
        return result

    @staticmethod
    def _get_attrs_dict(attrs: str) -> dict:
        return {vales.split()[1][:4]: vales.split()[0] for vales in attrs.split(',')}

    def save(self, post_data: dict):
        collection = self.db['cian_1_room']
        collection.insert_one(post_data)


if __name__ == '__main__':
    site_urls = [
        'https://www.cian.ru/cat.php?currency=2&deal_type=sale&engine_version=2&maxprice=8000000&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1',
        'https://www.cian.ru/cat.php?currency=2&deal_type=sale&engine_version=2&maxprice=8900000&minprice=8000001&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1',
        'https://www.cian.ru/cat.php?currency=2&deal_type=sale&engine_version=2&maxprice=9800000&minprice=8900001&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1',
        'https://www.cian.ru/cat.php?currency=2&deal_type=sale&engine_version=2&maxprice=11000000&minprice=9800001&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1'
    ]

    for site_url in site_urls:
        time.sleep(1800)
        parser = CianParse(site_url)
        parser.run()

    # site_url = ''
    # site_url = ''
    # site_url = ''
    # site_url = ''
    # site_url = 'https://www.cian.ru/cat.php?currency=2&deal_type=sale&engine_version=2&maxprice=13000000&minprice=11000001&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1'
    # site_url = 'https://www.cian.ru/cat.php?currency=2&deal_type=sale&engine_version=2&maxprice=20000000&minprice=13000001&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1'
    # site_url = 'https://www.cian.ru/cat.php?currency=2&deal_type=sale&engine_version=2&minprice=20000001&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1'
    # parser = CianParse(site_url)
    # parser.run()

    # site_urls = [
    #     'https://www.cian.ru/cat.php?currency=2&deal_type=sale&engine_version=2&maxprice=10000000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1',
    #     'https://www.cian.ru/cat.php?currency=2&deal_type=sale&engine_version=2&maxprice=11000000&minprice=10000001&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1',
    #     'https://www.cian.ru/cat.php?currency=2&deal_type=sale&engine_version=2&maxprice=12000000&minprice=11000001&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1',
    #     'https://www.cian.ru/cat.php?currency=2&deal_type=sale&engine_version=2&maxprice=13000001&minprice=12000001&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1'
    # ]
    # for site_url in site_urls:
    #     time.sleep(1800)
    #     parser = CianParse(site_url)
    #     parser.run()
