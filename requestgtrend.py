import os
import time
import json
import random

import requests
import pandas as pd
import plotly.offline
import plotly.graph_objects as go

from urllib.parse import urlencode
from pandas.io.json._normalize import nested_to_record


class GtrendReq:
    GET_METHOD = 'get'
    POST_METHOD = 'post'
    GENERAL_URL = 'https://trends.google.com/trends/api/explore'
    INTEREST_OVER_TIME_URL = 'https://trends.google.com/trends/api/widgetdata/multiline'
    RELATED_QUERIES_URL = 'https://trends.google.com/trends/api/widgetdata/relatedsearches'

    def __init__(self, hl='en-US', tz=360, geo='US', retries=1):
        self.headers = dict()
        self.hl = hl
        self.tz = tz
        self.geo = geo
        self.retries = max(retries, 1)
        self.kw_list = list()
        self.prepare_post_url = str()
        self.prepare_gettrend_url = str()
        self.prepare_getrelatedtopic_url = str()
        # intialize widget payloads
        self.token_payload = dict()
        self.interest_over_time_widget = dict()
        self.interest_by_region_widget = dict()
        self.related_topics_widget_list = list()
        self.related_queries_widget_list = list()

    def _get_data(self, url, params, method=GET_METHOD, trim_chars=0):
        """Send a request to Google and return the JSON response as a Python object
        :param url: the url to which the request will be sent
        :param method: the HTTP method ('get' or 'post')
        :param trim_chars: how many characters should be trimmed off the beginning of the content of the response
            before this is passed to the JSON parser
        :param kwargs: any extra key arguments passed to the request builder (usually query parameters or data)
        :return:
        """
        # Retries mechanism. Activated when one of statements >0 (best used for proxy)
        i = 0
        while i < self.retries + 1:
            print("#=============================================================================\n")
            print("第{}次请求:".format(i + 1))
            if method == GtrendReq.POST_METHOD:
                response = requests.post(url, params=params, headers=self.headers,
                                         timeout=(50, 60))  # DO NOT USE retries or backoff_factor here
                print(response.url)
                print(response.status_code)
                print(response.text)
            else:
                response = requests.get(url, params=params,
                                        timeout=(50, 60))  # DO NOT USE retries or backoff_factor here
                print(response.url)
                print(response.status_code)
                print(response.text)
            if response.status_code == 200:
                print("\n#=============================================================================")
                break
            else:
                time.sleep(random.uniform(3, 5))
                i += 1
        # check if the response contains json and throw an exception otherwise
        # Google mostly sends 'application/json' in the Content-Type header,
        # but occasionally it sends 'application/javascript
        # and sometimes even 'text/javascript
        if response.status_code == 200 and 'application/json' in \
                response.headers['Content-Type'] or \
                'application/javascript' in response.headers['Content-Type'] or \
                'text/javascript' in response.headers['Content-Type']:
            # trim initial characters
            # some responses start with garbage characters, like ")]}',"
            # these have to be cleaned before being passed to the json parser
            content = response.text[trim_chars:]
            # parse json
            return json.loads(content)
        else:
            # error
            try:
                response.raise_for_status()
            except:
                print('The request failed: Google returned a '
                      'response with code {0}.'.format(response.status_code))
                return None

    def build_payload(self, kw_list, cat=0, timeframe='today 5-y', geo='', gprop=''):
        """Create the payload for related queries, interest over time and interest by region"""
        if gprop not in ['', 'images', 'news', 'youtube', 'froogle']:
            raise ValueError('gprop must be empty (to indicate web), images, news, youtube, or froogle')
        self.kw_list = kw_list
        self.geo = geo or self.geo
        self.token_payload = {
            'hl': self.hl,
            'tz': self.tz,
            'req': {'comparisonItem': [], 'category': cat, 'property': gprop}
        }

        # build out json for each keyword
        for kw in self.kw_list:
            keyword_payload = {'keyword': kw, 'geo': self.geo, 'time': timeframe}
            self.token_payload['req']['comparisonItem'].append(keyword_payload)
        # requests will mangle this if it is not a string
        self.token_payload['req'] = json.dumps(self.token_payload['req'], separators=(',', ':'))
        self.prepare_post_url = urlencode(self.token_payload, safe=":,")
        self._tokens()
        return

    def _tokens(self):
        """Makes request to Google to get API tokens for interest over time, interest by region and related queries"""
        # make the request and parse the returned json
        # r = requests.post(self.GENERAL_URL, params=self.prepare_post_url, headers=self.headers)
        # print(r.text)
        # resultjson = json.loads(r.text[4:])
        widget_dicts = self._get_data(
            GtrendReq.GENERAL_URL,
            self.prepare_post_url,
            method=GtrendReq.POST_METHOD,
            trim_chars=4,
        )['widgets']
        # order of the json matters...
        first_region_token = True
        # clear self.related_queries_widget_list and self.related_topics_widget_list
        # of old keywords'widgets
        self.related_queries_widget_list[:] = []
        self.related_topics_widget_list[:] = []
        # assign requests
        for widget in widget_dicts:
            # print(widget['request'])
            # print(type(widget['request']))
            # widget['request']['userConfig']['userType'] = 'USER_TYPE_LEGIT_USER'
            if widget['id'] == 'TIMESERIES':
                self.interest_over_time_widget = widget
                # print(self.interest_over_time_widget)
            if widget['id'] == 'GEO_MAP' and first_region_token:
                self.interest_by_region_widget = widget
                first_region_token = False
            # response for each term, put into a list
            if 'RELATED_TOPICS' in widget['id']:
                self.related_topics_widget_list.append(widget)
            if 'RELATED_QUERIES' in widget['id']:
                self.related_queries_widget_list.append(widget)
        return

    def interest_over_time(self):
        """Request data from Google's Interest Over Time section and return a dataframe"""

        over_time_payload = {
            # convert to string as requests will mangle
            'hl': self.hl,
            'tz': self.tz,
            'req': json.dumps(self.interest_over_time_widget['request'], separators=(',', ':')),
            'token': self.interest_over_time_widget['token'],
            # 'tz': self.tz
        }
        # print(over_time_payload['req'])
        # make the request and parse the returned json
        self.prepare_gettrend_url = urlencode(over_time_payload, safe=":,")
        # r = requests.get(self.INTEREST_OVER_TIME_URL, params=self.prepare_gettrend_url)

        # print(r.url)
        # print(r.text)
        # req_json = json.loads(r.text[5:])
        req_json = self._get_data(
            url=GtrendReq.INTEREST_OVER_TIME_URL,
            params=self.prepare_gettrend_url,
            method=GtrendReq.GET_METHOD,
            trim_chars=5,
        )

        if req_json is None:
            return None
        df = pd.DataFrame(req_json['default']['timelineData'])
        if (df.empty):
            return df

        df['date'] = pd.to_datetime(df['time'].astype(dtype='float64'),
                                    unit='s')
        df = df.set_index(['date']).sort_index()
        # split list columns into seperate ones, remove brackets and split on comma
        result_df = df['value'].apply(lambda x: pd.Series(
            str(x).replace('[', '').replace(']', '').split(',')))
        # rename each column with its search term, relying on order that google provides...
        for idx, kw in enumerate(self.kw_list):
            # there is currently a bug with assigning columns that may be
            # parsed as a date in pandas: use explicit insert column method
            result_df.insert(len(result_df.columns), kw,
                             result_df[idx].astype('int'))
            del result_df[idx]

        if 'isPartial' in df:
            # make other dataframe from isPartial key data
            # split list columns into seperate ones, remove brackets and split on comma
            df = df.fillna(False)
            result_df2 = df['isPartial'].apply(lambda x: pd.Series(
                str(x).replace('[', '').replace(']', '').split(',')))
            result_df2.columns = ['isPartial']
            # Change to a bool type.
            result_df2.isPartial = result_df2.isPartial == 'True'
            # concatenate the two dataframes
            final = pd.concat([result_df, result_df2], axis=1)
        else:
            final = result_df
            final['isPartial'] = False

        return final

    def related_topics(self):
        """Request data from Google's Related Topics section and return a dictionary of dataframes

        If no top and/or rising related topics are found, the value for the key "top" and/or "rising" will be None
        """

        # make the request
        related_payload = dict()
        result_dict = dict()
        for request_json in self.related_topics_widget_list:
            # ensure we know which keyword we are looking at rather than relying on order
            try:
                kw = request_json['request']['restriction'][
                    'complexKeywordsRestriction']['keyword'][0]['value']
            except KeyError:
                kw = ''
            related_payload['hl'] = self.hl
            related_payload['tz'] = self.tz
            # convert to string as requests will mangle
            related_payload['req'] = json.dumps(request_json['request'], separators=(',', ':'))
            related_payload['token'] = request_json['token']

            self.prepare_getrelatedtopic_url = urlencode(related_payload, safe=":,")
            # print("self.prepare_getrelatedtopic_url:\n")
            # print(self.prepare_getrelatedtopic_url)
            # parse the returned json
            req_json = self._get_data(
                url=GtrendReq.RELATED_QUERIES_URL,
                method=GtrendReq.GET_METHOD,
                trim_chars=5,
                params=self.prepare_getrelatedtopic_url,
            )
            # print(req_json)
            if req_json is None:
                return None
            # top topics
            try:
                top_list = req_json['default']['rankedList'][0][
                    'rankedKeyword']
                df_top = pd.DataFrame(
                    [nested_to_record(d, sep='_') for d in top_list])
            except KeyError:
                # in case no top topics are found, the lines above will throw a KeyError
                df_top = None

            # rising topics
            try:
                rising_list = req_json['default']['rankedList'][1][
                    'rankedKeyword']
                df_rising = pd.DataFrame(
                    [nested_to_record(d, sep='_') for d in rising_list])
            except KeyError:
                # in case no rising topics are found, the lines above will throw a KeyError
                df_rising = None

            result_dict[kw] = {'rising': df_rising, 'top': df_top}
        return result_dict


def gtrendplotly(data, path):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=data["date"], y=data[data.columns.values[1]], mode="markers+lines", name="5year"))
    fig.add_trace(
        go.Scatter(x=data["date"][-52:], y=data[data.columns.values[1]][-52:], mode="markers+lines", name="12month"))
    fig.update_layout({'title': data.columns.values[1] + " 5年趋势图"}, yaxis_range=[0, 100])
    plotly.offline.plot(fig, filename=path + data.columns.values[1] + ".html", auto_open=False)


def configfunc():
    if os.path.exists("config.json"):
        fd = open("config.json")
    else:
        configpath = input("输入配置文件路径:\n")
        fd = open(configpath.replace("\'", "").replace("\"", ""))
    config = fd.read()
    fd.close()
    config = json.loads(config)
    return config


def main():
    config = configfunc()
    # print(config)
    os.environ["https_proxy"] = config["https_proxy"][random.randint(0, len(config["https_proxy"]) - 1)]
    print(os.environ["https_proxy"])
    headers = {key: config[key] for key in config.keys() if
               key == "user-agent" or key == "authority" or key == "cookie"}
    # print(headers)

    filepath = input("文件路径:\n")
    filepath = filepath.replace("\'", "").replace("\"", "")
    df_subj = pd.read_excel(filepath)
    parentpath = os.path.dirname(filepath) + "\\"
    gtrendhtmlpath = parentpath + os.path.splitext(os.path.basename(filepath))[0] + "-主题趋势报告" + "\\"

    # input(gtrendhtmlpath)
    if not os.path.isdir(gtrendhtmlpath):
        os.mkdir(gtrendhtmlpath)

    htmlfolderpath = gtrendhtmlpath + "趋势图" + "\\"
    if not os.path.isdir(htmlfolderpath):
        os.mkdir(htmlfolderpath)

    relatedtopicpath = gtrendhtmlpath + "相关主题" + "\\"
    if not os.path.isdir(relatedtopicpath):
        os.mkdir(relatedtopicpath)

    # df_subj = pd.read_excel("subject.xlsx")
    print(df_subj)
    notrendlist = list()
    trendlist = list()
    norelated5ylist = list()
    norelated7dlist = list()
    related5ylist = list()
    related7dlist = list()
    trendurl = list()
    topicurl = list()
    colname = df_subj.columns.values
    # '''
    print("\n开始获取数据...")
    time_start = time.time()
    trends = GtrendReq(hl=config["hl"], tz=config["tz"], retries=config["retries"])
    trends.headers = headers
    # input(trends.retries)
    for i in range(len(df_subj)):
        # if i != 1:
        #     continue
        '''
        subjectpath = gtrendhtmlpath + df_subj[colname[0]][i] + "\\"
        if not os.path.isdir(subjectpath):
            os.mkdir(subjectpath)
        '''

        #  五年趋势
        print("{}-{}:".format(i+1, df_subj[colname[0]][i]))
        print("请求5年POST...")
        trends.build_payload([df_subj[colname[0]][i]], timeframe="today 5-y")
        # '''
        print("\n请求5年趋势...")
        df = trends.interest_over_time()
        trendurl.append(trends.prepare_gettrend_url)
        # print(df)
        if df is None:
            notrendlist.append(df_subj[colname[0]][i] + "超时异常")
            norelated5ylist.append(df_subj[colname[0]][i] + "超时异常")
            norelated7dlist.append(df_subj[colname[0]][i] + "超时异常")
            continue

        if df.empty:
            print("该主题无趋势")
            notrendlist.append(df_subj[colname[0]][i])
            norelated5ylist.append(df_subj[colname[0]][i])
            # print(notrendlist)
            # print(norelated5ylist)
        else:
            # 绘制图形
            df = df.reset_index()
            gtrendplotly(df, htmlfolderpath)
            trendlist.append(df_subj[colname[0]][i])

            # 获取相关主题
            print("\n请求5年相关主题...")
            related_topics_5y = trends.related_topics()
            if related_topics_5y is None:
                norelated5ylist.append(df_subj[colname[0]][i] + "超时异常")
            else:
                if not related_topics_5y[df_subj[colname[0]][i]]["rising"].empty and not \
                        related_topics_5y[df_subj[colname[0]][i]]["top"].empty:
                    writer = pd.ExcelWriter(relatedtopicpath + df_subj[colname[0]][i] + "-risingtop.xlsx",
                                            engine='openpyxl')
                    if related_topics_5y[df_subj[colname[0]][i]]["rising"].empty:
                        print("该主题无5年相关上升主题")
                    else:
                        related_topics_5y[df_subj[colname[0]][i]]["rising"].to_excel(writer, index=False,
                                                                                     sheet_name="rising-5year")

                    if related_topics_5y[df_subj[colname[0]][i]]["top"].empty:
                        print("该主题无5年相关热门主题")
                    else:
                        related_topics_5y[df_subj[colname[0]][i]]["top"].to_excel(writer, index=False,
                                                                                  sheet_name="top-5year")
                    writer.save()
                    related5ylist.append(df_subj[colname[0]][i])
                else:
                    print("该主题无5年相关主题")
                    norelated5ylist.append(df_subj[colname[0]][i])
                    # input(norelated5ylist)
        # '''

        time.sleep(random.uniform(1, 3))
        # 7天主题
        print("\n请求7天POST...")
        trends.build_payload([df_subj[colname[0]][i]], timeframe="now 7-d")
        # 获取相关主题
        print("\n请求7天相关主题...")
        related_topics_7d = trends.related_topics()
        topicurl.append(trends.prepare_getrelatedtopic_url)
        if related_topics_7d is None:
            norelated7dlist.append(df_subj[colname[0]][i] + "超时异常")
        else:
            if not related_topics_7d[df_subj[colname[0]][i]]["rising"].empty or not \
                    related_topics_7d[df_subj[colname[0]][i]]["top"].empty:
                if os.path.exists(relatedtopicpath + df_subj[colname[0]][i] + "-risingtop.xlsx"):
                    writer = pd.ExcelWriter(relatedtopicpath + df_subj[colname[0]][i] + "-risingtop.xlsx",
                                            engine='openpyxl', mode='a')
                else:
                    writer = pd.ExcelWriter(relatedtopicpath + df_subj[colname[0]][i] + "-risingtop.xlsx",
                                            engine='openpyxl')
                if related_topics_7d[df_subj[colname[0]][i]]["rising"].empty:
                    print("该主题无7天相关上升主题")
                else:
                    related_topics_7d[df_subj[colname[0]][i]]["rising"].to_excel(writer, index=False,
                                                                                 sheet_name="rising-7day")

                if related_topics_7d[df_subj[colname[0]][i]]["top"].empty:
                    print("该主题无7天相关热门主题")
                else:
                    related_topics_7d[df_subj[colname[0]][i]]["top"].to_excel(writer, index=False,
                                                                              sheet_name="top-7day")
                writer.save()
                related7dlist.append(df_subj[colname[0]][i])
            else:
                print("该主题无7天相关主题")
                norelated7dlist.append(df_subj[colname[0]][i])
                # input(norelated7dlist)
        os.environ["https_proxy"] = config["https_proxy"][random.randint(0, len(config["https_proxy"]) - 1)]
        print(os.environ["https_proxy"]+"\n")
        # writer.save()
        # if len(os.listdir(subjectpath)) == 0:
        #     os.rmdir(subjectpath)
    time_end = time.time()

    resultwriter = pd.ExcelWriter(gtrendhtmlpath + "主题趋势报告.xlsx")
    result = pd.concat([pd.DataFrame({'有趋势图的主题': trendlist}),
                        pd.DataFrame({'5年有相关主题的主题': related5ylist}),
                        pd.DataFrame({'7天有相关主题的主题': related7dlist})], axis=1)
    result.to_excel(resultwriter, sheet_name="有趋势图", index=False)

    result1 = pd.concat([pd.DataFrame({'无趋势图的主题': notrendlist}),
                         pd.DataFrame({'5年无相关主题的主题': norelated5ylist}),
                         pd.DataFrame({'7天无相关主题的主题': norelated7dlist})], axis=1)
    result1.to_excel(resultwriter, sheet_name="无趋势图", index=False)

    result2 = pd.concat([df_subj,
                         pd.DataFrame({'获取趋势url': trendurl}),
                         pd.DataFrame({'获取7天主题url': topicurl})], axis=1)
    result2.to_excel(resultwriter, sheet_name="url", index=False)

    resultwriter.save()
    input("已生成报告, 耗时时间:{}, 平均耗时:{}, 按回车键结束".format(time_end - time_start, (time_end - time_start) / len(df_subj)))


if __name__ == "__main__":
    # test()
    main()

    '''
    df = pd.read_excel(r"F:\JetBrains\officeTools\googletrend\subject-主题趋势报告\主题趋势报告.xlsx")
    print(df)
    print(df.iloc[1, 2])
    tmp = list()
    df1 = pd.DataFrame()
    colname = df.columns.values
    for i in range(len(colname)):
        df1[colname[i]] = df[colname[i]].apply(lambda x: 0 if pd.isna(x) else i)
    print(df1)

    df1['结论'] = df1.apply(lambda x: x.sum(), axis=1)
    print(df1)
    '''
