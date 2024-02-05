import os

class GitHubScraperConfig:
    def __init__(self):
        self.baseUrl = 'https://api.github.com/'
        token = os.getenv('GITHUB_TOKEN')
        api_version = "2022-11-28"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": api_version
        }