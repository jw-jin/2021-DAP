import pandas as pd
import requests as rq
import os
import time
from dotenv import load_dotenv
from pymongo import MongoClient


def setup():
    # MongoDB 설정 https://somjang.tistory.com/entry/MongoDB-Python%EA%B3%BC-Pymongo%EB%A5%BC-%ED%99%9C%EC%9A%A9%ED%95%98%EC%97%AC-%EB%8D%B0%EC%9D%B4%ED%84%B0-%EC%B6%94%EA%B0%80%ED%95%98%EA%B3%A0-%EC%B6%9C%EB%A0%A5%ED%95%B4%EB%B3%B4%EA%B8%B0
    #my_client = MongoClient("mongodb://localhost:27017/")

    #mydb = my_client['db']

    # .env 파일을 통해 초기값 설정
    load_dotenv()
    global key, addr
    key = os.getenv('MyAPIKey')
    addr = os.getenv('RiotServerAddr')


def load_data(csv_name):
    csv_dir = os.getcwd() + csv_name
    pd.set_option('display.max_columns',None)
    df = pd.read_csv(csv_dir, encoding='cp949')
    print(df.head())

def get_userdata_tier(tier_name):
    action = '/lol/league/v4/'
    Tier = {'ch':'challengerleagues','gm':'grandmasterleagues','m':'masterleagues'}
    GameMode = 'RANKED_SOLO_5x5' # 5 vs 5 솔로랭크모드

    url = addr + action + Tier[tier_name] + '/by-queue/' + GameMode + '?api_key=' + key
    print(url)
    res = rq.get(url)
    print(res.text)

    df = pd.DataFrame(res.json())
    df.reset_index(inplace=True)# 수집한 데이터 index정리
    entries_df = pd.DataFrame(dict(df['entries'])).T # dict구조로 되어 있는 entries컬럼 풀어주기
    df = pd.concat([df, entries_df], axis=1) # 열끼리 결합
    df = df.drop(['index', 'queue', 'name', 'leagueId', 'entries', 'rank', 'veteran', 'inactive', 'freshBlood', 'hotStreak'], axis=1)

    df_acc = df
    df_acc['puuid'] = 0
    for i in range(len(df_acc)):
        if df_acc.iloc[i,-1] == 0:
            try:
                action = '/lol/summoner/v4/summoners/by-name/'
                url = addr + action + df_acc['summonerName'].iloc[i] + '?api_key=' + key
                res = rq.get(url)
                while res.status_code == 429:
                    print("failed:429")
                    time.sleep(5)
                    url = addr + action + df_acc['summonerName'].iloc[i] + '?api_key=' + key
                    res = rq.get(url)
                puuid_txt = res.json()['puuid']
                print(puuid_txt)
                df_acc.iloc[i,-1] = puuid_txt
            except:
                pass

    df_acc.to_csv(tier_name+'.csv',index=False,encoding = 'cp949')# 저장

def retry_userdata_tier(tier_name):
    df_acc = pd.read_csv(tier_name+".csv",encoding='cp949')
    for i in range(len(df_acc)):
        try:
            if df_acc.iloc[i,-1] == '0':
                action = '/lol/summoner/v4/summoners/by-name/'
                url = addr + action + df_acc['summonerName'].iloc[i] + '?api_key=' + key
                res = rq.get(url)
                print(res)
                while res.status_code == 429:
                    print("failed:429")
                    time.sleep(5)
                    url = addr + action + df_acc['summonerName'].iloc[i] + '?api_key=' + key
                    res = rq.get(url)
                account_id = res.json()['accountId']
                print(account_id)
                df_acc.iloc[i,-1] = account_id
        except:
            pass

    print("end")
    df_acc.to_csv(tier_name+'.csv',index=False,encoding = 'cp949')# 저장

def missing_value_userdata_process(tier_name):
    df = pd.read_csv(tier_name+".csv",encoding='cp949')
    df = df[df['puuid'] != '0']
    df.to_csv(tier_name+'_mv'+'.csv',index=False,encoding = 'cp949')# 저장

def get_matchid(tier_name):
    matchid_df = pd.DataFrame()
    df = pd.read_csv(tier_name+"_mv.csv",encoding='cp949') # 결측값 처리된 csv
    for i in range(len(df)):
        try:
            puuid = df['puuid'].iloc[i]
            print(puuid)
            addr = 'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/'
            url = addr + puuid + '/ids?type=ranked&start=0&count=100&api_key=' + key
            res = rq.get(url)

            while res.status_code == 429:
                print("fail:429")
                time.sleep(5)
                puuid = df['puuid'].iloc[i]
                addr = 'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/'
                url = addr + puuid + '/ids?type=ranked&start=0&count=100&api_key=' + key
                res = rq.get(url)

            print(res.json())
            matchid_df = pd.concat([matchid_df,pd.DataFrame(res.json())])
        except:
            print(i)
    matchid_df.to_csv(tier_name+'_matchid.csv',index=False,encoding = 'cp949')

def matchid_preprocess(tier_name):
    df = pd.read_csv(tier_name+"_matchid.csv",encoding='cp949')
    df = df.drop_duplicates(keep='first')
    df.to_csv(tier_name+'_matchid_pre'+'.csv',index=False,encoding = 'cp949')# 저장

def get_matchdata(tier_name,start_cnt,fin_cnt):
    #match_res = pd.DataFrame()
    match_res = pd.read_csv(tier_name+'_matchdata.csv', encoding='cp949')
    df_id = pd.read_csv(tier_name+'_matchid_pre.csv', encoding='cp949')
    if fin_cnt == -1: #전부 돌아가도록
        fin_cnt = len(df_id)
    print(fin_cnt)
    for i in range(start_cnt,fin_cnt):
        addr = 'https://asia.api.riotgames.com/lol/match/v5/matches/'
        url = addr + df_id['matchid'].iloc[i] + '?api_key=' + key
        res = rq.get(url)
        if res.status_code == 200: # response가 정상이면 바로 맨 밑으로 이동하여 정상적으로 코드 실행
            print(i)
            pass

        elif res.status_code == 429:
            print('api cost full : infinite loop start')
            print('loop location : ',i)
            start_time = time.time()

            while True: # 429error가 끝날 때까지 무한 루프
                if res.status_code == 429:
                    match_res.to_csv(tier_name+'_matchdata.csv',index=False,encoding = 'cp949')
                    print('10초대기 및 저장')
                    time.sleep(10)

                    res = rq.get(url)
                    print(res.status_code)

                elif res.status_code == 200: #다시 response 200이면 loop escape
                    print('total wait time : ', time.time() - start_time)
                    print('recovery api cost')
                    break

        elif res.status_code == 503: # 잠시 서비스를 이용하지 못하는 에러
            print('service available error')
            start_time = time.time()

            while True:
                if res.status_code == 503 or res.status_code == 429:

                    print('try 10 second wait time')
                    time.sleep(10)

                    res = rq.get(url)
                    print(res.status_code)

                elif res.status_code == 200: # 똑같이 response가 정상이면 loop escape
                    print('total error wait time : ', time.time() - start_time)
                    print('recovery api cost')
                    break
                else:
                    print("error")
                    print(res.status_code)
                    print("index:{}".format(i))
                    exit(-1)
        elif res.status_code == 403: # api갱신이 필요
            print('you need api renewal')
            print('break')
            break
        print(res.json())
        matchdata_df = pd.json_normalize(res.json()['info'], record_path=['participants'], meta=['gameCreation','gameId','gameMode','gameName','gameType','gameVersion','mapId','platformId','queueId'])
        matchdata_df = matchdata_df.loc[:,['gameId','summonerName','win','individualPosition','championId','championName','champLevel','kills','deaths','assists',
                       'item0','item1','item2','item3','item4','item5','item6',
                       'goldEarned','dragonKills','baronKills','objectivesStolen','teamId','totalDamageDealtToChampions','turretKills']]
        #pd.set_option('display.max_columns',None)
        #print(matchdata_df)
        match_res = pd.concat([match_res,matchdata_df])
    match_res.to_csv(tier_name+'_matchdata2.csv',index=False,encoding = 'cp949')
    print("end")

if __name__ == '__main__':
    setup()
    want_tier = 'm' # ch = 챌린져 gm = 그랜드마스터 m = master
    #get_userdata_tier(want_tier)
    #retry_userdata_tier(want_tier)
    #missing_value_userdata_process(want_tier)
    #get_matchid(want_tier)
    #matchid_preprocess(want_tier)
    get_matchdata(want_tier,111237,-1) #
    #3543

