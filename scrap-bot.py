import praw
import requests
import time
import datetime
import json
import pprint
import pandas as pd

from threading import Thread


URL = "https://api.pushshift.io/reddit/search/submission/?subreddit=dankmemes&sort=desc&sort_type=created_utc&after={}&before={}&size={}"


columns_to_drop = ["author_flair_background_color", "author_flair_css_class", "author_flair_richtext",
                    "author_flair_template_id", "author_flair_text", "author_flair_text_color", "author_flair_type",
                    "author_fullname", "author_patreon_flair", "can_mod_post", "contest_mode",
                    "domain", "full_link", "id", "is_crosspostable", "is_meta", "is_original_content",
                    "is_reddit_media_domain", "is_robot_indexable", "is_self", "is_video", "link_flair_background_color",
                    "link_flair_richtext", "link_flair_template_id", "link_flair_text", "link_flair_text_color",
                    "link_flair_type", "locked", "media_only", "no_follow", "permalink", "pinned", "pwls",
                    "subreddit", "subreddit_id", "subreddit_subscribers","subreddit_type", "suggested_sort",
                    "thumbnail", "title", "url", "whitelist_status", "wls"]


class Redditor:
    def __init__(self):
        self.the_vault = "data.json"

    def auth(self):
        self.reddit = praw.Reddit(client_id='xxxxxxxxxx',
                            client_secret='xxxxxxxxxxxxxxxxxxx',
                            user_agent='xxxx')

    def convert_to_csv(self):
        # with open(self.the_vault, 'r') as f:
        #     self.data = json.load(f)
        try:
            data = pd.read_csv('db-1.csv')
            df = pd.DataFrame(self.data['memes'])
            merged = pd.concat([data, df])
            merged.to_csv("db-1.csv")
        except Exception as e:            
            print(data.head())
            print(data.shape)
        finally:
            print(merged.head())
            print(merged.shape)


    def thread_meme(self, index, meme):
        submission = self.reddit.submission(id=meme['id'])
        print("i: {}, ID: {}, Upvotes {}".format(index, meme['id'], submission.score))
        self.data['memes'][index]['score'] = submission.score

    def get_post_update(self):
        """
        * Do data analysis on December dataset.
        * Get number of comments per post
        * Get if post is approved or not.
        * Get most frequent keywords in comments ex "REEE", "normie"
        """
        with open(self.the_vault, 'r') as f:
            self.data = json.load(f)

        # Update the meme upvotes
        for index, meme in enumerate(self.data['memes']):
            threads = []        
            process = Thread(target=self.thread_meme, args=[index, meme])
            process.start()
            threads.append(process)

            if index%500 == 0:
                for process in threads:
                    process.join()

                with open(self.the_vault, 'w') as f:
                    json.dump(self.data, f)

        with open(self.the_vault, 'w') as f:
            json.dump(self.data, f)


    def get_hourly_request(self, after, before, size, *args):
        response = requests.get(URL.format(after, before, size))
        print("Day {}, Hour Range {}-{}, Shitposts {}".format(args[0], args[1], args[2], len(response.json()['data'])))
        self.aggregate_memes.extend(response.json()['data'])

    def get_aggregate_history(self):
        # Complete Day
        self.aggregate_memes = []

        
        data = []
        for day in range(22, 25):
            threads = []
            for hour in range(0, 21, 4):
                after = datetime.datetime(2018, 12, day, hour)

                if (hour+4) % 24 == 0:
                    before = datetime.datetime(2018, 12, day+1, (hour+4) % 24)
                else:
                    before = datetime.datetime(2018, 12, day, hour+4)

                after = int(time.mktime(after.timetuple()))
                before = int(time.mktime(before.timetuple()))
                
                process = Thread(target=self.get_hourly_request, args=[after, before, 1000, day, hour, (hour+4) % 24])
                process.start()
                threads.append(process)


            for process in threads:
                process.join()

            data.extend(self.aggregate_memes)

        print(len(data))
        with open(self.the_vault, 'w') as f:
            json.dump({"memes": data}, f)

    def read(self):
        """
            Scrap post data from reddit from the past 7 days and compare the items that
            rose to the top with the ones that didn't.
        """
        dankmemes = self.reddit.subreddit('dankmemes')


r = Redditor()
r.auth()
r.get_aggregate_history()
r.get_post_update()    
r.convert_to_csv()