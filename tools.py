from vk_api import *
import json
from math import *
import time
import pandas as pd
import requests
import re
import pickle as pk

never_constant = time.time()


class VkParser():
    """main parser class"""
    def __init__(self, passfile=None, login=None, password=None):
        if (passfile is not None):
            with open(passfile, encoding="utf8") as in_file:
                data = in_file.read()
                logins = json.loads(data)
                login, password = logins[0]

        self.ErrorCode = None
        self.wall_collect = 0

        self.session = VkApi(login, password, api_version="5.89")
        try:
            self.session.auth(token_only=True)
        except Exception as e:
            print("500" + str(e))
            self.ErrorCode = 1
            return

        self.ErrorCode = 0
        self.api = self.session.get_api()
        self.tools = VkTools(self.session)

    def format_addresses(self, addresses):
        """Возвращает из htt...com/xxx  xxx для всех адресов из списка"""
        res = []
        for page in addresses:
            temp = page[page.rfind("/") + 1:]
            if (temp[0:2] != "id" and temp.isnumeric()):
                temp = "id" + temp
            res.append(temp)
        print("Formating completed")
        return res

    def collect_easy_features(self, page_ids=None):
        """Collect easy features, means download from vk"""
        res = []
        for i in range(0, len(page_ids), 1000):
            res.extend(self.collect_easy_features_1000(page_ids[i: i + 1000]))
        return res

    def get_creation_date(self, pg_id):
        """Creation date downloader, returns linux-type time"""
        r = requests.get(url="https://vk.com/foaf.php", params={"id": pg_id})
        ee = re.search(':created dc:date="([\d, T, \+, \-, :]*)"', r.text)
        if (ee is None):
            return never_constant
        s = ee.group(1)
        tm = (int(i) for i in [s[0:4], s[5:7], s[8:10], s[11:13], s[14:16], s[17:19], 0, 0, 0])
        tm = tuple(tm)
        return time.mktime(tm)

    def collect_easy_features_1000(self, page_ids):
        """collect 1000 of easy features, made for optimization"""
        # for more: vk.com/dev/objects/user
        print("Start Collecting 1000")
        # state 0 - страница забанена или удалена, 1 - страница закрыта
        # 2 не собирается признак стены, 3 - все отлично
        f2 = []

        # text_field_names = ["about", "activities", "books", "games",
        #                     "interests", "music", "movies", "quotes", "tv"]
        pg_opts = ",".join(["city", "connections", "contacts", "counters", "crop_photo", "domain",
                            "education", "folowers_count", "last_seen", "music", "movies",
                            "photo_max_orig", "relatives", "relation", "schools",
                            "screen_name", "status", "universities", "verified"])

        # for more see vk_api lib
        with VkRequestsPool(self.session) as pool:
            for i, page_id in enumerate(page_ids):
                f2.append(pool.method("users.get", {"user_ids": page_id, "fields": pg_opts}))

        f1 = []
        for i in range(len(f2)):
            if (f2[i].ok is False or len(f2[i].result) < 1):
                print("!!!! Err " + str(page_ids[i]) + " !!! " + str(f2[i].error))
                f1.append({"screen_name": page_ids[i], "deactivated": True, "state": -1})
            else:
                f1.append(f2[i].result[0])

        print("Main info collected")

        for pg in f1:
            if ("deactivated" not in pg):
                pg["creation_date"] = self.get_creation_date(pg["id"])

        print("Dates collected")

        if (self.wall_collect != 0):
            # collection of wall and photos, ie complex operations
            print("Photo Collection")
            # last photo
            with VkRequestsPool(self.session) as pool:
                for pg in f1:
                    if ("deactivated" in pg):
                        continue
                    if ("photos" in pg["counters"]):
                        photos_num = pg["counters"]["photos"]
                        pg["first_photo_date"] = pool.method("photos.getAll",
                                                             {"owner_id": pg["id"],
                                                              "offset": photos_num - 1})

            for pg in f1:
                if ("deactivated" in pg):
                    continue
                if ("photos" in pg["counters"] and pg["counters"]["photos"] > 0):
                    pg["first_photo_date"] = pg["first_photo_date"].result["items"][0]["date"]
                else:
                    pg["first_photo_date"] = never_constant

            print("Wall Collection")
            # wall collection
            # collecting only last 100, cause of efficency and wall limit
            # for more vk api limits
            with VkRequestsPool(self.session) as pool:
                for pg in f1:
                    if ("deactivated" not in pg and
                        (pg["is_closed"] is False or
                         pg["can_access_closed"] is True)):

                        pg["wall_records"] = pool.method(method="wall.get",
                                                         values={"owner_id": pg["id"],
                                                                 "count": 100})

            print("Records collected")

            for pg in f1:
                # collecting records information
                if ("deactivated" not in pg and
                    (pg["is_closed"] is False or
                     pg["can_access_closed"] is True)):

                    if (pg["wall_records"].ok):
                        pg["wall_records"] = pg["wall_records"].result
                        pg["counters"]["posts"] = pg["wall_records"]["count"]
                        pg["state"] = 3
                        pg["wall_records"] = pg["wall_records"]["items"]
                        if (len(pg["wall_records"]) > 0):
                            pg["first_post_date"] = pg["wall_records"][-1]["date"]
                        else:
                            pg["first_post_date"] = never_constant

                    else:
                        print(pg["wall_records"].error)
                        pg["state"] = 5

        # state filling
        for pg in f1:
            if ("state" in pg and pg["state"] == -1):
                pass
            elif ("deactivated" in pg):
                pg["state"] = 0
            elif (pg["is_closed"] is True and pg["can_access_closed"] is False):
                pg["state"] = 1
            elif("state" not in pg):
                pg["state"] = 2

            # self.tools.get_all("photos.getAlbums", 2, {owner_id : page_id})
        print("Collection Completed 1000")
        return f1

    def extract_easy_features(self, f1):
        """Extraction of features, a part of normalization"""

        print("Started extraction")

        res = []
        for pg in f1:
            res.append({})
            cur = res[-1]
            cur["screen_name"] = pg["screen_name"]
            cur["state"] = pg["state"]
            if (pg["state"] == 1 or pg["state"] == 2 or pg["state"] == 3):
                cur["id"] = pg["id"]
                cur["created"] = pg["creation_date"]
                # страница закрыта
                cur["created_ago"] = time.time() - cur["created"]
                if ("groups" in pg["counters"]):
                    cur["count_gr"] = pg["counters"]["groups"]
                else:
                    cur["count_gr"] = 0
                if ("pages" in pg["counters"]):
                    cur["count_gr"] += pg["counters"]["pages"]
                if ("subscriptions" in pg["counters"]):
                    cur["count_gr"] += pg["counters"]["subscriptions"]

                if ("posts" in pg["counters"]):
                    cur["count_wa"] = pg["counters"]["posts"]
                elif ("wall_records" in pg):
                    cur["count_wa"] = len(pg["wall_records"])
                else:
                    cur["count_wa"] = -1
                if ("friends" in pg["counters"]):
                    cur["count_fr"] = pg["counters"]["friends"]
                else:
                    cur["count_fr"] = -1
                if ("crop_photo" in pg):
                    temp = pg["crop_photo"]["crop"]
                    cur["photo_crop"] = int(abs(temp["x"] - 0) > 5) + int(abs(temp["y"] - 0) > 5) +\
                        int(abs(temp["x2"] - 100) > 5) + int(abs(temp["y2"] - 100) > 5)
                else:
                    cur["photo_crop"] = 0
                if ("photos" in pg["counters"]):
                    cur["photo_exist"] = pg["counters"]["photos"] > 0
                else:
                    cur["photo_exist"] = 0
                cur["count_status"] = 5
                if ("status" in pg):
                    for i in ["com", "http", "ru", "org", "www"]:
                        if (i in pg["status"]):
                            cur["count_status"] -= 1
                if (pg["screen_name"][0:2] != "id"):
                    cur["name_changed"] = 1
                else:
                    cur["name_changed"] = 0

            if (pg["state"] == 2 or pg["state"] == 3):
                if ("first_photo_date" not in pg):
                    pg["first_photo_date"] = never_constant
                if ("first_post_date" not in pg):
                    pg["first_post_date"] = -never_constant

                cur["created_diff"] = min(abs(pg["first_photo_date"] - pg["first_post_date"]),
                                          abs(pg["first_photo_date"] - pg["creation_date"]),
                                          abs(pg["first_post_date"] - pg["creation_date"]))

                if ("followers" in pg["counters"] and
                    "friends" in pg["counters"] and
                    pg["counters"]["friends"] > 0):

                    cur["folow_to_friends"] = pg["counters"]["followers"] /\
                        pg["counters"]["friends"]
                else:
                    cur["folow_to_friends"] = 1.0
                cur["count_av"] = 0
                if ("audios" in pg["counters"]):
                    cur["count_av"] += pg["counters"]["audios"]
                if ("videos" in pg["counters"]):
                    cur["count_av"] += pg["counters"]["videos"] * 2
                cur["count_con"] = 0
                # инста просто часто используется для рекламы
                for i in ["skype", "facebook", "twitter", "livejournal"]:
                    if (i in pg):
                        cur["count_con"] += 1

            if (cur["state"] == 3 and cur["count_wa"] < 1):
                cur["state"] = 2

            if (cur["state"] == 3):
                likes = 0
                views = 0
                counter = 0
                cur_date = time.time()
                date_difs = []
                for record in pg["wall_records"]:
                    if ("views" in record):
                        likes += record["likes"]["count"]
                        views += record["views"]["count"]
                        counter += 1
                    date_difs.append(cur_date - record["date"])
                    cur_date = record["date"]
#                 if (len(date_difs) > 0):
#                     cur["wall_median"] = median(date_difs)
#                 else:
#                     cur["wall_median"] = 100000
                dd = [0, 0, 0, 0]
                for i in date_difs:
                    if (i < 7200):
                        dd[0] += 1
                    elif (i < 3600 * 24):
                        dd[1] += 1
                    elif (i < 3600 * 48):
                        dd[2] += 1
                    else:
                        dd[3] += 1
                cur["wall_diffs"] = dd
                if (views == 0):
                    views = 1
                cur["wall_likeview"] = likes / views
                if (cur["count_fr"] == 0):
                    cur["count_fr"] = 10**6
                cur["wall_likefriends"] = likes / cur["count_fr"] / counter * 100
                cur["wall_viewfriends"] = views / cur["count_fr"] / counter
        print("Completed extraction")
        return pd.DataFrame(res)

    def save(self, features, file_name="data_file.csv"):
        df1 = pd.read_csv(file_name)
        df = pd.concat([df1, features])
        df.to_csv(file_name, index=False)

    def save_wt_clear(self, features, file_name="data_file.csv"):
        features.to_csv(file_name, index=False)

    def read(self, file_name="data_file.csv"):
        return pd.read_csv(file_name)

    def extract_final(self, df):
        lst = ["screen_name", "count_av", "count_gr", "count_wa", "created_diff", "created_ago",
               "folow_to_friends", "name_changed", "photo_crop",
               "state", "wall_diffs", "wall_likefriends", "wall_viewfriends"]

        lst = [a for a in lst if a in df]
        res = df[lst]
        return res

    def normalize(self, df):
        # normalization, hardcoded to exclude sklearn/other heavy libs
        res = pd.DataFrame()
#         df = df.fillna(value = -1)
#         def nan_ob(val, func):
#             if (val == pd.NaN):
#                 return NaN
#             else:
#                 return func(val)
        if ("screen_name" in df):
            res["screen_name"] = df["screen_name"]
        else:
            res = pd.DataFrame(columns=["screen_name", "state"])
            return res

        features = {"count_av": lambda x: 0 if (x > 0 and x < 700) else
                    (1 if (x == 0 or x > 1000) else ((x - 700) / 300)),
                    "count_gr": lambda x: 0 if (x < 50) else (1 if x > 150 else (x - 50) / 100),
                    "count_wa": lambda x: 0 if (x != 0) else 1,
                    "created_diff": lambda x: 0 if (x > 604800) else 1,
                    "created_ago": lambda x: 0 if (x > 1209600) else 1,
                    "folow_to_friends": lambda x: 0 if x > 0.1 else 1,
                    "name_changed": lambda x: 1 - x,
                    "photo_crop": lambda x: 0 if (x > 0) else 1,
                    "state": lambda x: x,
                    "wall_diffs": lambda x: 1 if (x[0] == max(x)) else 0,
                    "wall_likefriends": lambda x: 0 if (x > 1.0) else
                    (1 if (x < 0.3) else (((1.0 - x) / 0.7) ** 2)),
                    "wall_viewfriends": lambda x: 0 if (x > 0.5) else
                    (1 if (x < 0.2) else (((0.5 - x) / 0.3) ** 2))
                    }

        for feat in features.items():
            if (feat[0] in df):
                res[feat[0]] = df[feat[0]].\
                    apply(lambda x: feat[1](x) if isinstance(x, list) or not isnan(x) else 0.5)
            else:
                res[feat[0]] = pd.Series((0.5 for _ in range(len(df))), name=feat[0])

        return res

    def predict(self, df):
        # prediction function, splits df into 4 states
        dfs = []
        dfs.append(pd.concat([df[df["state"] == -1][["screen_name", "state"]],
                              pd.Series(([-1, False] for _ in range(len(df[df["state"] == -1]))),
                             name="res")], axis=1))
        dfs.append(pd.concat([df[df["state"] == 0][["screen_name", "state"]],
                              pd.Series(([-1, False] for _ in range(len(df[df["state"] == 0]))),
                             name="res")], axis=1))
        for i in range(1, 4):
            # loading from model file, coefs got by sklearn linear
            with open("models/model{}.vkp".format(i), "rb") as fl:
                pred = pk.load(fl)
            df_t = df[df["state"] == i]
            df_t.reset_index(drop=True, inplace=True)
            predicted = pd.Series(pred.predict(df_t.loc[:, df.columns != "screen_name"]),
                                  name="res")
            dfs.append(pd.concat([df_t[["screen_name", "state"]], predicted], axis=1))
        return dfs


class PredictorV2():
    """linear predictor to exclude sklearn
    coefs are loaded from model file"""
    def __init__(self):
        self.coef_ = []
        self.add_coef_ = 0
        self.e_coef_ = 0
        self.edge = 0.41
        return

    def single_predictor(self, row):
        linear = sum([cval * val for cval, val in zip(self.coef_, row)]) + self.add_coef_
        t = [linear, linear > self.edge]
#         t = (1 / (1 + e ** (-l))) + self.e_coef_
        return t

    def predict(self, data):
        res = [self.single_predictor(row[1]) for row in data.iterrows()]
        return res
