import os

class StackOverflowScraperConfig:
    def __init__(self):
        self.baseUrl = "https://api.stackexchange.com/"
        self.api_version = "2.3/"
        self.api = "search/advanced"
        self.batch = 100
