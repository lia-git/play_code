from collections import Counter

import pandas as pd
import matplotlib.pyplot as plt
import redis
import json
import numpy as np
import matplotlib.dates as mdates
import datetime as dt
def get_time_pd(start, end):
    tempList = pd.date_range(start=start, end=end, freq='D')
    date_pd = pd.DataFrame({"date": tempList, "value": pd.Series([1] * len(tempList))})
    date_pd["date"] = date_pd["date"].dt.strftime("%Y-%m-%d")
    print date_pd.index
    # date_pd.index = date_pd["date"
    print date_pd.index
    day_list = [str(i)[:10] for i in tempList]

    day_dict = {}.fromkeys(day_list, 1)
    return date_pd, day_list, day_dict


def fetch_distinct_store():
    l = []
    air_1 = pd.read_csv("data/air_reserve.csv", header=0)
    air_2 = pd.read_csv("data/hpg_reserve.csv", header=0)
    relation = pd.read_csv("data/store_id_relation.csv", header=0)
    air_2 = pd.merge(air_2, relation, on="hpg_store_id")
    l1 = set(air_1["air_store_id"].tolist())
    l2 = set(air_2["air_store_id"].tolist())
    l0 = l1 & l2
    l = l1 | l2
    return list(set(l)), l0, l1, l2


def main_1(part, s, e):
    date_pd, day_list, day_dict = get_time_pd(s, e)
    # import datetime as dt

    # day_list = [dt.datetime.strptime(i, '%Y-%m-%d').date() for i in day_list]

    air_reserve_all = pd.read_csv("data/air_reserve.csv", header=0)
    air_reserve_all["visit_datetime"] = air_reserve_all["visit_datetime"].str[:-9]

    hpg_reserve_all = pd.read_csv("data/hpg_reserve.csv", header=0)
    hpg_reserve_all["visit_datetime"] = hpg_reserve_all["visit_datetime"].str[:-9]
    relation = pd.read_csv("data/store_id_relation.csv", header=0)
    hpg_reserve_all = pd.merge(hpg_reserve_all, relation, on="hpg_store_id")
    air_visit_data_all = pd.read_csv("data/air_visit_data.csv", header=0)
    cross_reserve = pd.merge(air_reserve_all, hpg_reserve_all, on="air_store_id")
    cross_reserve = cross_reserve[cross_reserve["visit_datetime_x"] == cross_reserve["visit_datetime_y"]]
    cross_reserve["visit_datetime"] = cross_reserve["visit_datetime_x"]
    cross_reserve["reserve_visitors"] = cross_reserve["reserve_visitors_x"] + cross_reserve["reserve_visitors_y"]
    cross_reserve.drop(
        ['visit_datetime_x', 'reserve_datetime_x', 'reserve_visitors_x', 'hpg_store_id', 'visit_datetime_y',
         'reserve_datetime_y', 'reserve_visitors_y'], axis=1, inplace=True)
    print(cross_reserve.columns.tolist())
    print(cross_reserve.size)
    if "1" in part:
        np_reserve = np.array([0]*183)
        np_actual = np.array([0]*183)
    else:
        np_reserve = np.array([0]*297)
        np_actual = np.array([0]*297)
    for store in stores:
        if store in l0:
            air_reserve = cross_reserve[
                (cross_reserve["air_store_id"] == store) & (s <= cross_reserve["visit_datetime"]) & (
                        cross_reserve["visit_datetime"] < e)]
        else:
            if store in l1:
                # air_reserve = air_reserve_all[air_reserve_all["air_store_id"] == store]

                air_reserve = air_reserve_all[
                    (air_reserve_all["air_store_id"] == store) & (s <= air_reserve_all["visit_datetime"]) & (
                            air_reserve_all["visit_datetime"] < e)]


            else:
                air_reserve = hpg_reserve_all[
                    (hpg_reserve_all["air_store_id"] == store) & (s <= hpg_reserve_all["visit_datetime"]) & (
                            hpg_reserve_all[
                                "visit_datetime"] < e)]  # air_reserve[t] = pd.to_datetime(air_reserve[t].str[:-9])
        # air_reserve[t] = pd.to_datetime(air_reserve[t],unit='D')
        air_visit_data = air_visit_data_all[
            (air_visit_data_all["air_store_id"] == store) & (s <= air_visit_data_all["visit_date"]) & (
                    air_visit_data_all["visit_date"] < e)]
        print(air_reserve["visit_datetime"].max())
        print(air_reserve["visit_datetime"].min())

        print air_reserve.dtypes
        # air_reserve = pd.merge(air_reserve,date_pd,left_on="visit_datetime",right_on="date")
        day_reserve = air_reserve.groupby(["visit_datetime"]).agg({'reserve_visitors': sum})
        day_reserve_dict = day_reserve.to_dict()["reserve_visitors"]
        x = dict(Counter(day_reserve_dict) | Counter(day_dict))
        pd_1 = pd.DataFrame(x.items(), columns=['date', 'number'])
        pd_1.sort_values(["date"], inplace=True)
        visitors_reserve = (pd_1['number'] - 1).tolist()

        # day_reserve.reset_index(inplace=True)
        # print(day_reserve.columns.tolist())

        # air_visit_data = pd.merge(air_visit_data,date_pd,left_on="visit_date",right_on="date")
        # air_visit_data = pd.merge(air_visit_data,date_pd.rename(columns = {"visit_datetime":"visit_date"}),on="visit_date")
        # relation = pd.read_csv("data/store_id_relation.csv", header=0)
        # air_visit_data = pd.merge(air_visit_data, relation, on="air_store_id")
        # air_visit_data["visit_date"] = pd.to_datetime(air_visit_data["visit_date"])
        day_visit = air_visit_data.groupby(["visit_date"]).agg({"visitors": sum})
        day_visit_dict = day_visit.to_dict()["visitors"]
        x = dict(Counter(day_visit_dict) | Counter(day_dict))
        pd_x = pd.DataFrame(x.items(), columns=['date', 'number'])
        pd_x.sort_values(["date"], inplace=True)
        visitors_actual = (pd_x['number'] - 1).tolist()
        np_reserve += np.array(visitors_reserve)
        np_actual += np.array(visitors_actual)
        dic = {"reserve": visitors_reserve, "actual": visitors_actual}
        redis_conn.hmset(name=store, mapping={part: json.dumps(dic)})
    dc = {"reserve": np_reserve.tolist(), "actual": np_actual.tolist()}

    redis_conn.hmset(name="all_data_set", mapping={part: json.dumps(dc)})

    # day_visit.reset_index(inplace = True)
    # visitors_actual =  day_visit["visitors"].tolist()
    # x = list(range(0, 517))
    # loop =  517 / 30
    # plt.figure(figsize=(30, 50), dpi=150)
    # for i in range(loop):
    #     index = (loop +1) *100 + 1+(loop+1)
    #     plt.subplot(loop+1,1,i+1)
    #     plt.plot(x[i*30:(i+1)*30],visitors_reserve[i*30:(i+1)*30],label='reserve line',linewidth=1,color='r',marker='o',
    #              markerfacecolor='blue',markersize=1)
    #     plt.plot(x[i*30:(i+1)*30],visitors_actual[i*30:(i+1)*30],label='actual line')
    #     # plt.xlabel('Plot Number')
    #     # plt.ylabel('Important var')
    #     plt.title('{}th month'.format(i+1))
    #
    # plt.savefig("compare.png",dpi=120)
    # plt.close()
    # plt.xticks(tempList,rotation=45)
    # plt.legend()
    import datetime as dt

    # day_list = [dt.datetime.strptime(i, '%Y-%m-%d').date() for i in day_list]


# plt.show()
# print()

def plot_fig(p,day_list,visitors_reserve,visitors_actual):
    day_list = [dt.datetime.strptime(i,'%Y-%m-%d').date() for i in  day_list]

    plt.figure(figsize=(300, 50), dpi=100)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d/%Y'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator())

    plt.plot(day_list, visitors_reserve[:-1], label='reserve line', linewidth=1, color='r', marker='o',
             markerfacecolor='blue', markersize=4)
    plt.plot(day_list, visitors_actual[:-1], label='actual line', linewidth=1, color='b', marker='*',
             markerfacecolor='red', markersize=4)

    plt.gcf().autofmt_xdate()
    plt.grid(True, linestyle="-.")
    plt.savefig("compare_{}.png".format(p), dpi=100)



def get_data_from_redis(p):

    v = redis_conn.hget("all_data_set",p)
    return json.loads(v)


def main_2():
    tempList = pd.date_range(start='2016-01-01', end='2017-05-31')

    day_list = [str(i)[:10] for i in tempList]

    day_dict = {}.fromkeys(day_list, 1)

    air_reserve = pd.read_csv("data/hpg_reserve.csv", header=0)
    for t in ["visit_datetime", "reserve_datetime"]:
        air_reserve[t] = air_reserve[t].str[:-9]
        # air_reserve[t] = pd.to_datetime(air_reserve[t].str[:-9])
        # air_reserve[t] = pd.to_datetime(air_reserve[t],unit='D')
    print(air_reserve["visit_datetime"].max())
    print(air_reserve["visit_datetime"].min())

    print air_reserve.dtypes
    day_reserve = air_reserve.groupby(["visit_datetime"]).agg({'reserve_visitors': sum})
    day_reserve_dict = day_reserve.to_dict()["reserve_visitors"]
    x = dict(Counter(day_reserve_dict) | Counter(day_dict))
    pd_x = pd.DataFrame(x.items(), columns=['date', 'number'])
    pd_x.sort_values(["date"], inplace=True)
    visitors_reserve = (pd_x['number'] - 1).tolist()

    # day_reserve.reset_index(inplace=True)
    # print(day_reserve.columns.tolist())

    air_visit_data = pd.read_csv("data/air_visit_data.csv", header=0)
    relation = pd.read_csv("data/store_id_relation.csv", header=0)
    air_visit_data = pd.merge(air_visit_data, relation, on="air_store_id")
    # air_visit_data["visit_date"] = pd.to_datetime(air_visit_data["visit_date"])
    day_visit = air_visit_data.groupby(["visit_date"]).agg({"visitors": sum})
    day_visit_dict = day_visit.to_dict()["visitors"]
    x = dict(Counter(day_visit_dict) | Counter(day_dict))
    pd_x = pd.DataFrame(x.items(), columns=['date', 'number'])
    pd_x.sort_values(["date"], inplace=True)
    visitors_actual = (pd_x['number'] - 1).tolist()

    redis_conn.hmset(stores)
    '''
    
    #
    # day_visit.reset_index(inplace = True)
    # visitors_actual =  day_visit["visitors"].tolist()
    x = list(range(0, 517))
    # loop =  517 / 30
    # plt.figure(figsize=(30, 50), dpi=150)
    # for i in range(loop):
    #     index = (loop +1) *100 + 1+(loop+1)
    #     plt.subplot(loop+1,1,i+1)
    #     plt.plot(x[i*30:(i+1)*30],visitors_reserve[i*30:(i+1)*30],label='reserve line',linewidth=1,color='r',marker='o',
    #              markerfacecolor='blue',markersize=1)
    #     plt.plot(x[i*30:(i+1)*30],visitors_actual[i*30:(i+1)*30],label='actual line')
    #     # plt.xlabel('Plot Number')
    #     # plt.ylabel('Important var')
    #     plt.title('{}th month'.format(i+1))
    #
    # plt.savefig("compare.png",dpi=120)
    # plt.close()
    plt.figure(figsize=(300, 50), dpi=150)
    import matplotlib.dates as mdates
    # plt.xticks(tempList,rotation=45)
    # plt.legend()


    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d/%Y'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator())

    plt.plot(day_list, visitors_reserve, label='reserve line', linewidth=1, color='r', marker='o',
             markerfacecolor='blue', markersize=4)
    plt.plot(day_list, visitors_actual, label='actual line', linewidth=1, color='b', marker='*',
             markerfacecolor='red', markersize=4)

    plt.gcf().autofmt_xdate()
    plt.grid(True, linestyle="-.")
    plt.savefig("compare1.png", dpi=120)

    # plt.show()
    
    '''
    print()


# air_store_info = pd.read_csv("data/air_store_info.csv", header=0)
def save_redis(ins):
    redis_conn.lpush("valid_stores", *ins)


# def show_store():


if __name__ == '__main__':
    stores, l0, l1, l2 = fetch_distinct_store()
    redis_pool = redis.ConnectionPool(host="127.0.0.1", port=6379)
    redis_conn = redis.Redis(connection_pool=redis_pool)
    # save_redis(stores)
    v = get_data_from_redis("p2")
    tempList = pd.date_range(start="2016-07-01", end="2017-04-22", freq='D')
    days = [str(i)[:10] for i in tempList]
    plot_fig("p2",days,v["actual"],v["reserve"])




    # main_1("p2", "2016-07-01", "2017-04-23")
    # main_1("p1", "2016-01-01", "2016-07-01")
    # main_2()
