import requests, sys, time, os, argparse, configparser, csv, re

config = configparser.ConfigParser()
config.read_file(open("config.cfg"))
api_key = config.get("API", "API_KEY")


# Parameters to specify in request url
YOUTUBE_PARAMETERS = [
    "part",
    "chart",
    "regionCode",
    "maxResults",
    "key",
    "pageToken",
]

# Characters to exclude when preprocessing videos' features before reading to files
excluded_characters = ["\n", '"']

# Data header
header = [
    "video_id",
    "published_time",
    "trending_date",
    "channel_id",
    "channel_title",
    "video_title",
    "category_id",
    "thumbnail_link",
    "view_count",
    "like_count",
    "dislike_count",
    "favorite_count",
    "comment_count",
    "rating_disable",
    "comment_disable",
    "tags",
    "description",
]


def read_country_code_file(country_code_file_path):
    """
    function to read in file containing country codes
    """
    with open(country_code_file_path) as file:
        country_codes = [x.rstrip() for x in file]
    return country_codes


def get_request(**kwargs):
    """
    function to prepare a base get request and receive json response
    """

    parameters = []

    for k, v in kwargs.items():
        if k in YOUTUBE_PARAMETERS:
            parameters.append(f"{k}={v}")

    base_request_url = (
        f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet&chart=mostPopular&key={api_key}"
    )
    request_url = base_request_url + "&" + "&".join(parameters)
    response = requests.get(request_url)

    if response.status_code == 429:
        print("Excessive requests. Continue later")
        sys.exit()

    response = response.json()
    npt = response.get("nextPageToken")
    return response


def preprocess_video_element(video_element):
    """
    function to preprocess each video element before writing to file
    """
    for c in excluded_characters:
        preprocessed_video_element = str(video_element).replace(c, " ")
    return f'"{preprocessed_video_element}"'


def get_data_by_country(country_code):
    """
    function to get data by country for each request and combine data from all request to 1 list
    """
    data_by_country = []

    page_token = None
    response = get_request(regionCode=country_code, maxResults=50)
    page_token = response.get("nextPageToken")
    # get data by country
    data_per_request = response.get("items", [])
    data_by_country.extend(data_per_request)

    while page_token is not None:
        response = get_request(regionCode=country_code, maxResults=50, pageToken=page_token)
        page_token = response.get("nextPageToken")
        # get data by country
        data_per_request = response.get("items", [])
        data_by_country.extend(data_per_request)
    return data_by_country


def write_country_data_to_file(data_by_country):
    """
    function to get video elements, preprocess them and write to file
    """
    country_data = []
    country_data.append(",".join(header))
    for i in data_by_country:
        video_id = i.get("id", "")
        published_time = i["snippet"].get("publishedAt", "")
        channel_id = i["snippet"].get("channelId", "")
        channel_title = i["snippet"].get("channelTitle", "")
        video_title = i["snippet"].get("title", "")
        description = i["snippet"].get("description", "")
        thumbnail_link = i["snippet"].get("thumbnails", dict()).get("default", dict()).get("url", "")
        # description = description.rstrip('"\n"')
        # description = re.sub('\s*\n\s*', '', description)
        tags = "|".join(i["snippet"].get("tags", ""))
        category_id = i["snippet"].get("categoryId")
        view_count = i["statistics"].get("viewCount", 0)
        like_count = i["statistics"].get("likeCount")
        dislike_count = i["statistics"].get("dislikeCount")
        favorite_count = i["statistics"].get("favoriteCount", 0)
        comment_count = i["statistics"].get("commentCount")
        rating_disable = True if like_count is None and dislike_count is None else False
        comment_disable = True if comment_count is None else False
        trending_date = time.strftime("%y.%d.%m")

        video_elements = [
            video_id,
            published_time,
            trending_date,
            channel_id,
            channel_title,
            video_title,
            category_id,
            thumbnail_link,
            view_count,
            like_count,
            dislike_count,
            favorite_count,
            comment_count,
            rating_disable,
            comment_disable,
            tags,
            description,
        ]

        video_row = [preprocess_video_element(x) for x in video_elements]

        video_row = ",".join(video_row)
        country_data.append(video_row)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"write data of country {country_code} to file: Starting")


    # with open(f"{output_dir}/{time.strftime('%y.%d.%m')}_{country_code}_data.csv", "w", newline="", encoding="utf-8") as csvfile:
    #     spamwriter = csv.writer(csvfile, delimiter=" ", quotechar="|", quoting=csv.QUOTE_MINIMAL)
    #     for row in country_data:
    #         spamwriter.writerow(row)

    with open(
        f"{output_dir}/{time.strftime('%y.%d.%m')}_{country_code}_videos.csv", "w+", encoding="utf-8", newline=""
    ) as file:
        for row in country_data:
            file.write(f"{row}\n")

    print(f"write data of country {country_code} to file: Completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--country_code_path",
        help="Path to the list of countries codes file, use country_codes.txt in the same directory by default",
        default="country_codes.txt",
    )
    parser.add_argument("--output_dir", help="Path to the folder that outputted files are saved", default="output/")

    args = parser.parse_args()
    output_dir = args.output_dir
    country_codes = read_country_code_file(args.country_code_path)
    for country_code in country_codes:
        data_by_country = get_data_by_country(country_code)
        write_country_data_to_file(data_by_country)
