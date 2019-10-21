import asyncio
import csv
import aiohttp
import requests
import bs4


class CommentParser:
    def __init__(self, ednpoint="obzor-bukmekerskoj-kontory-ligastavok"):
        self.comments = [["src", "bookie_name", "comments", "name", "score", "like", "dislike", "date"]]
        self.ednpoint = ednpoint
        self.url = f"https://bookmaker-ratings.ru/review/{self.ednpoint}/all-feedbacks/"
        self.ajax_query_url = "https://bookmaker-ratings.ru/wp-admin/admin-ajax.php"
        self.post_id = 0
        self.num_page = 0
        self.bookie_name = None

    def get_soup(self):
        """
        Получаем bs4 объект искомой страницы
        :return:
        """
        response = requests.get(self.url)
        return bs4.BeautifulSoup(response.content, "lxml")

    def set_bookie_name(self, soup):
        """
        Узнаём название букмекерской конторы
        :param soup:
        :return:
        """
        self.bookie_name = soup.find("h1").text.split(" конторе ")[1]

    def set_post_id(self, soup):
        """
        Устанавливаем id данного букмекера
        :return:
        """
        self.post_id = int(soup.find("button", {"id": "all-feedbacks-more-btn"})["data-postid"])

    def set_num_page(self, soup):
        """
        Устанавливаем кол-во страниц отзывов
        :param soup:
        :return:
        """
        self.num_page = int(soup.find("button", {"id": "all-feedbacks-more-btn"})["data-total-pages"])

    def none_check(self, obj):
        """
        Проверка на None, иногда сервер отдаёт путстые комментарии
        :return:
        """
        return None if obj is None else obj.text.strip()

    async def set_comment(self, element):
        """
        Кладём данные о комментариях в свойств comments, чтобы потом построить csv
        :param element:
        :return:
        """
        name = self.none_check(element.find("a", {"class": "namelink"}))
        comment = self.none_check(element.find("div", {"class": "use-default-ui"}))
        score = self.none_check(element.find("span", {"class": "num"}))
        like = self.none_check(element.find("a", {"class": "like"}))
        dislike = self.none_check(element.find("a", {"class": "dislike"}))
        date = self.none_check(element.find("div", {"class": "date"}))
        self.comments.append([self.url, self.bookie_name] + [comment, name, score, like, dislike, date])

    async def get_comments(self, session, data):
        """
        Асинхронно кидаем данные на ендпоинт и получает html c комментариями постранично
        :param session:
        :param data:
        :return:
        """
        async with session.post(self.ajax_query_url, data=data) as response:
            res = await response.read()
            soup = bs4.BeautifulSoup(res, "lxml").find_all("div", {"class": "single"})
            for x in soup:
                await self.set_comment(x)

    async def get_headers(self, page):
        """
        Асинхронно формируем параметры запроса
        :param page:
        :return:
        """
        data = {
            "action": "feedbacks_items_page",
            "postID": self.post_id,
            "page": page
        }
        return data

    async def create_tacks(self):
        """
        Генерируем и запускаем асинхронные задачи для парсинга
        :return:
        """
        tasks = []
        async with aiohttp.ClientSession() as session:
            for x in range(1, self.num_page + 1):
                data = await self.get_headers(x)
                task = asyncio.create_task(self.get_comments(session, data))
                tasks.append(task)
            await asyncio.gather(*tasks)

    def create_csv(self):
        with open(f'csv_reports/{self.ednpoint}-data.csv', 'w') as csvfile:
            filewriter = csv.writer(csvfile)
            filewriter.writerows(self.comments)

    def run(self):
        soup = self.get_soup()
        self.set_num_page(soup)
        self.set_post_id(soup)
        self.set_bookie_name(soup)
        asyncio.run(self.create_tacks())
        self.create_csv()
