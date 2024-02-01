import dotenv
import sys
from helpers.gh_scraper import GitGubScraper
from helpers.gh_scraper_config import GitHubScraperConfig
from helpers.so_scraper import StackOverflowScraper
from helpers.so_scraper_config import StackOverflowScraperConfig

def main():
    dotenv.load_dotenv()
    scraper_arg = sys.argv[1:2][0]
    if scraper_arg == 'gh':
        gh_scraper_config = GitHubScraperConfig()
        gh_scraper = GitGubScraper(gh_scraper_config)
        gh_scraper.start()
    elif scraper_arg == 'so':
        so_scraper_config = StackOverflowScraperConfig()
        so_scraper = StackOverflowScraper(so_scraper_config)
        so_scraper.start()

if __name__ == '__main__':
    main()