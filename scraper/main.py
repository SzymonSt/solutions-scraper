import dotenv
import sys
from helpers.gh_scraper import GitGubScraper
from helpers.gh_scraper_config import GitHubScraperConfig

def main():
    dotenv.load_dotenv()
    scraper_arg = sys.argv[1:2][0]
    if scraper_arg == 'gh':
        gh_scraper_config = GitHubScraperConfig()
        gh_scraper = GitGubScraper(gh_scraper_config)
        gh_scraper.start()
    elif scraper_arg == 'so':
        pass

if __name__ == '__main__':
    main()