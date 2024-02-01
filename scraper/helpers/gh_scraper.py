
import requests
import logging
import time
from datetime import datetime
import pandas as pd
from helpers.gh_scraper_config import GitHubScraperConfig
from helpers.issue import Issue


class GitGubScraper:
    def __init__(self, config: GitHubScraperConfig, iteration: int = 0, since: str = None):
        # Initilize config
        self.config = config
        
        # Initilize parameters
        self.rate_limit = 5000
        self.num_of_collected_issues = 0
        self.iterations = iteration
        self.since = since

        # Initilize helpers
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler('./logs/ghscraper.log')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)

    def start(self):
        issues_batch = []
        if self.since:
            repos_link = self.config.baseUrl + "repositories?since=" + self.since
        else:
            repos_link = self.config.baseUrl + "repositories"
        while repos_link:
            repos, repos_link = self.get_ghobjects(repos_link)
            if not repos and not repos_link:
                self.logger.error("Failed to get repos, exiting... to save requests quota.")
                break
            elif not repos and repos_link:
                self.logger.warn("Failed to get repos, but got link to next page.")
                continue


            for repo in repos:
                issues_link = f"{self.config.baseUrl}repos/{repo['full_name']}/issues"
                while issues_link:
                    issues, issues_link = self.get_ghobjects(issues_link)
                    if not issues and not issues_link:
                        self.logger.error("Failed to get issues, exiting... to save requests quota.")
                        break
                    elif not issues and issues_link:
                        self.logger.warn("Failed to get issues, but got link to next page.")
                        continue


                    for issue in issues:
                        comments_link = f"{self.config.baseUrl}repos/{repo['full_name']}/issues/{issue['number']}/comments?sort=created&direction=asc"
                        comments, _ = self.get_ghobjects(comments_link)
                        if not comments:
                            self.logger.warn("Failed to get comments.")
                            continue
                        featured_comment = None
                        for comment in comments:
                            if (comment["author_association"] == "OWNER"):
                                featured_comment = comment["body"]
                                break
                            elif (comment["author_association"] == "COLLABORATOR"):
                                featured_comment = comment["body"]
                                break
                            elif (comment["author_association"] == "MEMBER"):
                                featured_comment = comment["body"]
                                break
                            elif (comment["author_association"] == "CONTRIBUTOR"):
                                featured_comment = comment["body"]
                                break
                        if featured_comment and issue["body"]:
                            issues_batch.append(Issue(issue["title"], issue["body"], issue["url"], featured_comment))
            self.iterations += 1
            self.num_of_collected_issues += len(issues_batch)
            self.logger.info(f"Next Page: {repos_link} .Processed {self.iterations} iterations. Request Quota left {self.rate_limit}. Collected issues: {self.num_of_collected_issues}\n")
            df = pd.DataFrame([vars(issue) for issue in issues_batch], columns=["title", "description", "url", "featured_answer"], dtype=object)
            df.to_parquet(f"./output/issues-gh-{self.iterations}.parquet")
            issues_batch = []
            time.sleep(1.5)


    def get_ghobjects(self, link):
        status_code = 0
        retries = 5
        while status_code != 200:
            resp = requests.get(link, headers=self.config.headers)
            status_code = resp.status_code
            if self.rate_limit <= 5:
                reset_time = int(resp.headers.get("X-RateLimit-Reset"))
                reset_date = datetime.fromtimestamp(reset_time)
                self.logger.info(f"Rate limit reached. Sleeping until {reset_date.strftime('%Y-%m-%d %H:%M:%S')}.")
                time.sleep(reset_date.timestamp() - datetime.now().timestamp() + 5.0)
            if retries < 1:
                return None, None
            if status_code != 200:
                link_header = resp.headers.get("Link")
                try:
                    links = link_header.split(",")
                    link = next((link.split(';')[0].replace("<",'').replace(">",'') for link in links if "rel=\"next\"" in link), None)
                    return None, link
                except:
                    self.logger.warn(f"Failed to execute request to {link}. Status code: {resp.status_code}. Headers: {resp.headers.__dict__}\n")
                    time.sleep(5.0)
                    retries -= 1
        try:
            objects = resp.json()
        except:
            objects = None
        self.rate_limit = int(resp.headers.get("X-RateLimit-Remaining"))
        link_header = resp.headers.get("Link")
        try:
            links = link_header.split(",")
            link = next((link.split(';')[0].replace("<",'').replace(">",'') for link in links if "rel=\"next\"" in link), None)
        except:
            link = None
        return objects, link