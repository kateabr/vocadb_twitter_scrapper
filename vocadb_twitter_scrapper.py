import os
import requests
from re import findall, match
from tqdm import tqdm
import urllib
from datetime import datetime
import json
import pandas as pd
from functools import reduce
from halo import Halo


def date_is_after(datetime1, datetime2):
    date1 = datetime1.date()
    date2 = datetime2.date()
    if date1.year > date2.year:
        return True
    elif date1.year < date2.year:
        return False
    if date1.month > date2.month:
        return True
    elif date1.month < date2.month:
        return False
    if date1.day > date2.day:
        return True
    else:
        return False


class LinkParser:
    regex: str
    entry_ids: {}
    links: {}

    def __init__(self, regex):
        self.regex = regex
        self.entry_ids = set([])
        self.links = set([])


def fail_exit(message):
    with (spinner := Halo()):
        spinner.fail(message)
        exit()


def validate_date(date_text):
    try:
        return datetime.fromisoformat(date_text.strip())
    except ValueError:
        fail_exit("Incorrect date format: should be YYYY-MM-DD")


def get_date(timestamp):
    return validate_date(timestamp).date()


def update_report(report, tweet, link, resolution):
    processed_tweets.add(tweet['url'])
    return pd.concat(
        [report,
         pd.DataFrame({"Tweet             ": [f"{tweet['url']} [{get_date(tweet['date'])}]"],
                       "Date": [get_date(tweet['date'])], "Link": [link],
                       "Resolution   ": [resolution]})])


def search_by_media_link(media_link):
    return requests.get(
        f"https://vocadb.net/api/songs/findDuplicate?pv[]={urllib.request.quote(media_link)}&getPVInfo=true").json()


def is_participant(song_data):
    return ("tags" in song_data.keys()) \
        and (9141 in song_data["tags"]) \
        and (match(f".*(vocadb\.net\/E\/{event_id}).*", song_data["notes"]["original"])
             or match(f".*(vocadb\.net\/E\/{event_id}).*", song_data["notes"]["english"]))


def multiple_events(song_data):
    return ("tags" in song_data.keys()) \
        and (8275 in song_data["tags"]) \
        and (match(f".*(vocadb\.net\/E\/{event_id}).*", song_data["notes"]["original"])
             or match(f".*(vocadb\.net\/E\/{event_id}).*", song_data["notes"]["english"]))


event_id = int(input("Event id: "))

print()
with (spinner := Halo()):
    spinner.start(text='Loading event data...')
    event = requests.get(f"https://vocadb.net/api/releaseEvents/{event_id}?fields=WebLinks,Series").json()

    hashtags = []

    if "webLinks" in event.keys():
        hashtags_temp = [match(r"https://twitter.com/hashtag/([^\?/]+)", link["url"]) for link in event["webLinks"]]
        hashtags = [urllib.request.unquote(hashtag.group(1)) for hashtag in hashtags_temp if hashtag is not None]
    if (len(hashtags) == 0) and ("series" in event.keys()):
        event_series = requests.get(
            f"https://vocadb.net/api/releaseEventSeries/{event['series']['id']}?Fields=WebLinks").json()
        if "webLinks" not in event_series.keys():
            spinner.fail(f"No weblinks associated with {event['name']} or its series!")
            exit()
        hashtags_temp = [match(r"https://twitter.com/hashtag/([^\?/]+)", link["url"]) for link in
                         event_series["webLinks"]]
        hashtags = [urllib.request.unquote(hashtag.group(1)) for hashtag in hashtags_temp if hashtag is not None]
    if len(hashtags) == 0:
        spinner.fail(f"No Twitter hashtags associated with {event['name']}!")
        exit()
    spinner.succeed('Loading event data... Done.')

print()
print(f"Event name: {event['name']}")
if 'endDate' in event.keys():
    print(f"Start date: {str(get_date(event['date']))}")
    print(f"End date: {str(get_date(event['endDate']))}")
else:
    print(f"Date: {str(get_date(event['date']))}")
print(f"Discovered following hashtags: {', '.join(['#' + h for h in hashtags])}")
print()

start_date = str(input("Search since (def=earliest mention): "))
if start_date != "":
    validate_date(start_date)
end_date = str(input("Search until (def=current date): "))
if end_date != "":
    validate_date(start_date)
max_results = 100
start_at = 0
custom_start_at = input("Skip how many tweets (def=0; newer tweets come first): ")
if custom_start_at != "":
    if not custom_start_at.isdigit():
        fail_exit(f"'{custom_start_at}' is not a number")
    if int(custom_start_at) < 0:
        fail_exit("Negative tweet offset")
    start_at = int(custom_start_at)
custom_max_results = input("Process how many tweets (def=100): ")
if custom_max_results != "":
    if not custom_max_results.isdigit():
        fail_exit(f"{custom_max_results} is not a number")
    if int(custom_max_results) < 0:
        fail_exit("Negative max results number")
    max_results = int(custom_max_results)

filenames = {hashtag: f"tweets_{hashtag}_{str(datetime.now()).replace(' ', '_').replace(':', '-')}.json" for hashtag in
             hashtags}

print()

for hashtag in hashtags:
    with (spinner := Halo()):
        spinner.start(text=f"Scraping #{hashtag}...")
        cmd = f"snscrape --max-results {start_at + max_results} " \
              f"--jsonl{' --since ' + start_date if start_date != '' else ''} twitter-hashtag {hashtag}"
        os.system(f'{cmd} > {filenames[hashtag]}')
        spinner.succeed(text=f"Scraping #{hashtag}... Done.")

results = pd.DataFrame({"Tweet             ": [], "Date": [], "Link": [], "Resolution   ": []})
processed_tweets = set([])

for hashtag, file in filenames.items():
    print()
    print(f"#{hashtag}")
    b_format = "|{bar}| {percentage:3.0f}% {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]"
    with open(file, 'r', newline='', encoding='utf8') as tweets_csv:
        tweets = tweets_csv.readlines()

    if end_date != "":
        for idx, tweet in enumerate(tweets):
            parsed_tweet = json.loads(tweet)
            if date_is_after(datetime.fromisoformat(parsed_tweet["date"]), datetime.fromisoformat(end_date)):
                continue
            else:
                tweets = tweets[idx:]
                break

    for tweet in tqdm(tweets, bar_format=b_format):
        parsed_tweet = json.loads(tweet)
        if parsed_tweet["url"] in processed_tweets:
            continue
        tweet_text = parsed_tweet["rawContent"]
        links = findall(r"https://t.co/[^\s]+", tweet_text)
        parsers = {LinkParser(r"https://www\.nicovideo\.jp/watch"),
                   LinkParser(r"https://www\.youtube\.com/watch"),
                   LinkParser(r"https://www\.bilibili\.com/video/")}
        for link in links:
            try:
                with requests.Session() as session:
                    resp = session.head(link, allow_redirects=True)
            except Exception as _:
                results = update_report(results, parsed_tweet, "???", "Invalid link !!")
                continue
            for parser in parsers:
                if match(parser.regex, resp.url) is not None:
                    lookup = search_by_media_link(resp.url)
                    parser.entry_ids = parser.entry_ids.union(
                        set([match["entry"]["id"] for match in lookup["matches"] if match["matchProperty"] == "PV"]))
                    parser.links.add(resp.url)
        all_entry_ids = reduce(lambda x, y: x.union(y), [p.entry_ids for p in parsers])
        entry_data = {}
        for parser in parsers:
            # link attached to an entry (or entries)
            if (len(parser.links) > 0) and (len(all_entry_ids) > 0):
                # link not attached to some of the possible entries
                if len(diff := all_entry_ids.difference(parser.entry_ids)) > 0:
                    for media_link in parser.links:
                        for entry_id in diff:
                            results = update_report(
                                results,
                                parsed_tweet,
                                media_link,
                                f"Possibly missing from entry: https://vocadb.net/S/{entry_id}  !")
                # link attached to all available entries
                else:
                    # check if the event is filled out correctly
                    for song_id in all_entry_ids:
                        if song_id not in entry_data.keys():
                            db_response = requests.get(f"https://vocadb.net/api/songs/{song_id}/for-edit")
                            if not db_response.ok:
                                fail_exit("Failed to receive response from the database:\n" + db_response.text)
                            entry_data[song_id] = db_response.json()
                        song_data = entry_data[song_id]
                        participated = is_participant(song_data)
                        multiple = multiple_events(song_data)
                        if "releaseEvent" not in song_data.keys() and not participated and not multiple:
                            results = update_report(results, parsed_tweet, f"https://vocadb.net/S/{song_id}",
                                                    "Event missing !!")
                        elif "releaseEvent" in song_data.keys() \
                                and song_data["releaseEvent"]["id"] != event_id \
                                and not participated \
                                and not multiple:
                            results = update_report(results, parsed_tweet, f"https://vocadb.net/S/{song_id}",
                                                    "Possibly wrong event  !")
            # link not attached to any entries
            elif len(parser.links) > 0:
                for media_link in parser.links:
                    results = update_report(results, parsed_tweet, media_link, "Media link not in the database !!")

print()
if len(results) > 0:
    print(results.sort_values(by="Date", ascending=False)
          .drop(["Date"], axis=1)
          .to_string(index=False))
else:
    print("No new links right now♪")

for file in filenames.values():
    os.remove(file)
