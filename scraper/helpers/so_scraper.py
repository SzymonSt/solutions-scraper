import requests
import time
import logging
import html
from datetime import datetime, timedelta
import pandas as pd
from helpers.so_scraper_config import StackOverflowScraperConfig
from helpers.issue import Issue


class StackOverflowScraper:
    def __init__(self, config: StackOverflowScraperConfig, iteration: int = 0, since: int = 1):
        # Initilize config
        self.config = config

        # Initilize parameters
        self.iterations = iteration
        self.page = since
        self.rate_limit = 300
        self.param_string_template = f"?pagesize={self.config.batch}&order=desc&sort=activity&site=stackoverflow&answers=1&filter=!*236eb_eL9rai)MOSNZ-6D3Q6ZKb0buI*IVotWaTb"

        # Initilize helpers
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler('./logs/soscraper.log')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)

    def start(self):
        issues_batch = []
        param_string = self.param_string_template + f"&page={self.page}"
        posts_link = f"{self.config.baseUrl}{self.config.api_version}{self.config.api}{param_string}"
        while posts_link:
            posts, posts_link = self.get_ghobjects(posts_link)
            if not posts and not posts_link:
                self.logger.error("Failed to get posts, exiting... to save requests quota.")
                break
            elif not posts and posts_link:
                self.logger.warn("Failed to get posts, but got link to next page.")
                continue
            for post in posts:
                description = post["body_markdown"]
                url = post["link"]
                title = post["title"]
                featured_answer = next((answer["body_markdown"] for answer in post["answers"] if answer["is_accepted"] and answer["score"] > 0), None)
                if not featured_answer:
                    answers_sorted_by_score = sorted(post['answers'], key=lambda x: x['score'], reverse=True)
                    if answers_sorted_by_score[0]['score'] > 0:
                        featured_answer = answers_sorted_by_score[0]["body_markdown"]
                if featured_answer and description:
                    featured_answer = html.unescape(featured_answer)
                    issues_batch.append(Issue(title, description, url, featured_answer))
            if len(issues_batch) >= self.config.batch * 10:   
                self.iterations += 1
                self.logger.info(f"Next Page: {self.page} .Processed {self.iterations} iterations\n")
                df = pd.DataFrame([vars(issue) for issue in issues_batch], columns=["title", "description", "url", "featured_answer"], dtype=object)
                df.to_parquet(f"./output/issues-so-{self.iterations}.parquet")
                issues_batch = []
            time.sleep(1.25)
    
    
    def get_ghobjects(self, link):
        status_code = 0
        retries = 5
        while status_code != 200:
            resp = requests.get(link)
            status_code = resp.status_code
            if self.rate_limit <= 5:
                reset_time = 3600 * 24
                reset_date = datetime.now() + timedelta(seconds=reset_time)
                self.logger.info(f"Rate limit reached. Sleeping until {reset_date.strftime('%Y-%m-%d %H:%M:%S')}.")
                time.sleep(reset_time + 5)
            if retries < 1:
                return None, None
            if status_code != 200:
                self.logger.warn(f"Failed to execute request to {link}. Status code: {resp.status_code}. Headers: {resp.headers.__dict__}\n")
                time.sleep(5.0)
                retries -= 1
        try:
            body = resp.json()
            objects = body["items"]
            self.rate_limit = int(body["quota_remaining"])
        except:
            objects = None
        self.page += 1
        param_string = self.param_string_template + f"&page={self.page}"
        link = f"{self.config.baseUrl}{self.config.api_version}{self.config.api}{param_string}"
        return objects, link