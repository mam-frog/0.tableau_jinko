
# # e-statで取得したデータの前処理を実施する

"""
About Module:
    e-statから国勢調査の都道府県市区町村別人口1980年から2020年のデータをTableauで可視化する前に前処理を実施

"""
# ## 0.ライブラリのインポート

from typing import Final
import pandas as pd 
import csv
import glob
import re
from IPython.display import display

from typing import List, Tuple, Dict
from nptyping import NDArray, Object

import warnings
warnings.simplefilter('ignore')

# ライブラリの設定変更
pd.options.display.max_rows = None
pd.options.display.max_columns = None


class DataPrepPopulation():
    """
    About this class：
        1980年から2020年までの個々のファイルに対して実施するデータ前処理を記載
        ・不要行の削除など
    
    Attributes:
        DATA_FILE_PATH（Final[str]): 国勢調査の人口に関するファイルを格納しているディレクトリパス
        DATA_CITY_PATH(Final[str]): 政令指定都市のファイルパス
        DATA_PATH_MENSEKI(Final[str]): 市区町村ごとの面積のファイルパス
        
    """

    def __init__(self):
        self.DATA_FILE_PATH: Final[str] = "../0.input_data/kokusei_research/"
        self.DATA_CITY_PATH: Final[str] = "../0.input_data/sub_data/政令指定都市区データ.xlsx"
        self.DATA_PATH_MENSEKI: Final[str]="../0.input_data/sub_data/R1_R4_all_mencho.csv"
        
    def read_file_prep(self,file_name: str) -> pd.DataFrame:
        
        """
        __init__で設定されたself.file_nameのファイルに対して前処理を実施する。
        get_times_data関数で使用されている。
        read_file_prep ⇒ (df_popu_del_sousu[DataFrame型]): ファイルを読み込んでデータ前処理を実施する

        Args:
            file_name[str]: get_times_data関数にて取得した国勢調査のfile名を代入する
        Returns:
            df_popu_del_sousu[DataFrame]: 国勢調査人口ファイルが前処理されたDataFrame
        Raises:
            なし
        Examples:
            class_data_prep = DataPrepPopulation.read_file_prep(file_name)

        Note:
            なし
        
        """
        # ファイルの読み込み
        with open(file_name, \
            "r", encoding="utf-8", errors="", newline="" ) as f:

            csv_result = csv.reader(f, delimiter=",")
            df_jinko = pd.DataFrame(csv_result)

        #何行まで不要な情報が入っているか確認する⇒0~26まで不要なので削除する
        df_jinko_del_top = df_jinko[27:]

        # カラム名を設定する
        list_col_name = [i for i in df_jinko_del_top.iloc[0]]
        df_jinko_del_top.columns = list_col_name

        # カラム名設定後、1行目を削除してカラム名を振り直す
        df_jinko_col = df_jinko_del_top[1:]
        df_jinko_col_idx = df_jinko_col.reset_index(drop=True)

        # 正規表現で以下の文字列が含む行を抽出する
        df_pref = df_jinko_col_idx \
            [df_jinko_col_idx["area_code"].str.contains("000")] \
            .drop_duplicates(subset=["area_code"])
            
        # 都道府県の辞書を作る
        dict_pref = {}
        for k,v in zip(df_pref["area_code"],df_pref[df_pref.columns[7]]):
            dict_pref[k[0:2]] = v
        
        # prefecture_codeとprefecture_nameを作成
        df_jinko_col_idx["pref_code"] = df_jinko_col_idx["area_code"].apply(lambda x: x[0:2])
        df_jinko_col_idx["pref_name"] = df_jinko_col_idx["pref_code"].map(dict_pref)

        # 都道府県の行は削除する
        df_jinko_del_pref = df_jinko_col_idx[df_jinko_col_idx["area_code"]!=df_jinko_col_idx["pref_code"] + "000"]

        # 表彰項目が"割合”のものを削除する
        df_popu_del_wariai = df_jinko_del_pref.loc[df_jinko_del_pref["tab_code"] == "020"]

        # 年齢３区分_時系列が”総数"を削除する & 男女時系列が"総数"を削除する
        df_popu_del_sousu = df_popu_del_wariai.loc[(df_popu_del_wariai["cat01_code"] != "100") & (df_popu_del_wariai["cat02_code"] != "100")]

        # 7つ目のカラムに都道府県名が含まれている場合prefecture_nameを使用してReplaceする
        for v in dict_pref.values():
            df_popu_del_sousu.loc[df_popu_del_sousu[df_popu_del_sousu.columns[7]].str.contains(v),df_popu_del_sousu.columns[7]] = \
                df_popu_del_sousu.loc[df_popu_del_sousu[df_popu_del_sousu.columns[7]].str.contains(v),df_popu_del_sousu.columns[7]].apply(lambda x: x.replace(v,""))

        return df_popu_del_sousu
    
    def get_times_data(self) -> pd.DataFrame:
        """
        read_file_prep関数を使用してフォルダ内のファイルをすべて前処理をして結合する処理を実施
        get_times_data ⇒(df_data_times[DataFrame型]): read_file_prep関数を使用して国勢調査のファイルを処理し、すべてのファイルの前処理後を結合する

        Args:
            なし
        Returns:
            df_data_times[DataFrame]: 国勢調査人口ファイルが前処理されたクロスセクションDataFrameを作成
        Raises:
            なし
        Examples:
            DataPrep = DataPrepPopulation()
            df_data_times = DataPrep.get_times_data()
        Note:
            なし
        """
        files = glob.glob(self.DATA_FILE_PATH + "*.csv")

        data_list=[]
        for file in files:
            df = self.read_file_prep(file)
            df.columns = ['tab_code', '表章項目', 'cat01_code', '年齢３区分_時系列', 'cat02_code', '男女_時系列',
            'area_code','市区町村', 'time_code', '時間軸（調査年）', 'unit', 'value',
            'annotation', 'pref_code', '都道府県']
            data_list.append(df)

        df_data_times = pd.concat(data_list,ignore_index=True)

        return df_data_times

    def ex_shi(self ,content: str) -> str:
        """
        ex_shi ⇒ (content[str]): 市区町村名に”市”と”区”が含まれていた場合に市までの文字列を抽出する
        get_times_data_prep関数内で使用。pandasの関数によるデータ処理で使用

         Args:
            content[str]:市区町村名 
        Returns:
            content[str]:市名
        Raises:
            なし
        Examples:
            ”大阪市北区”という文字列を”大阪市”のみ抽出する
        Note:
            なし
        """
        if "市" in content  and "区" in content :
            shi_number = content.find("市")
            return content[:shi_number+1]
        else:
            return content

    def ex_ku(self ,content: str) -> str:
        """
        ex_shi ⇒ (content[str]): 市区町村名に”市”と”区”が含まれていた場合に区の部分の文字列を抽出する
        get_times_data_prep関数内で使用。pandasの関数によるデータ処理で使用

         Args:
            content[str]:市区町村名 
        Returns:
            content[str]:区名
        Raises:
            なし
        Examples:
            ”大阪市北区”という文字列を”北区”のみ抽出する
        Note:
            なし
        """
        if "市" in content  and "区" in content :
            shi_number = content.find("市")
            return content[shi_number+1:]
        else:
            return content

    def get_times_data_prep(self, df_data_times: pd.DataFrame) -> pd.DataFrame:
        """
        get_times_data_prep ⇒ (df_data_times_del_tooku[DataFrame]): 
        取得した年すべてのデータ（get_times_data関数の出力値）に対して政令指定都市と特別区に関する処理、表記ゆれなどの前処理を実施。

        Args:
            df_data_times[DataFrame]:国勢調査の取得した年のファイルを前処理・結合したデータフレーム 
        Returns:
            df_data_times_del_tooku[DataFrame]:特別区と政令指定都市に対する前処理を実施した取得年全体のデータフレーム
        Raises:
            なし
        Examples:
            df_data_times_del_tooku = DataPrep.get_times_data_prep(df_data_times)
        Note:
            なし
        """

        # まず市区町村データに含まれる空白を除去する
        df_data_times["市区町村"] = df_data_times["市区町村"].str.replace(" ","").replace("　","")

        # 取得したExcelを政令指定都市の一覧と、政令指定都市の区の一覧にデータフレームを分ける。
        df_city = pd.read_excel(self.DATA_CITY_PATH,sheet_name =1,dtype={'団体コード':'object'})

        # area_codeを新しく作成
        df_city["area_code"] = df_city["団体コード"].apply(lambda x: str(x)[0:5])
        df_city = df_city.rename(columns={"都道府県名\n（漢字）":"都道府県名","市区町村名\n（漢字）":"市区町村名","都道府県名\n（ｶﾅ）":"都道府県名（ｶﾅ）","市区町村名\n（ｶﾅ）":"市区町村名（ｶﾅ）"})

        # df_cityの市区町村名をdf_data_timesにmergeする
        df_data_times_city = pd.merge(df_data_times, df_city[["area_code","市区町村名"]], how = "left", on = "area_code")

        # 市区町村名が埋まっている場合は市区町村を市区町村名と一致させる
        df_data_times_city.loc[~(df_data_times_city["市区町村名"].isnull()),"市区町村"] = df_data_times_city.loc[~(df_data_times_city["市区町村名"].isnull()),"市区町村名"] 

        # mergeしたデータを落とす
        df_data_times_city = df_data_times_city.drop(["市区町村名"],axis=1)

        # 市町村カラムを作成する（区は含まれない）
        df_data_times_city["市町村"] = df_data_times_city["市区町村"].apply(lambda x: self.ex_shi(x))

        # 区を埋める
        df_data_times_city["区"] = df_data_times_city["市区町村"].apply(lambda x: self.ex_ku(x))

        # 市区町村名と市町村が同じ場合は削除する。
        list_city = df_city.loc[(df_city["市区町村名"].str.contains("市") )& (~(df_city["市区町村名"].str.contains("区"))),"市区町村名" ].unique()
        df_data_times_del_city = df_data_times_city.loc[~(df_data_times_city["市区町村"].isin(list_city))]

        # 仙台を追加（仙台は1990年から区割りされている（1985年までは仙台市のみ）
        df_sendai = df_data_times_city.loc[(df_data_times_city["市区町村"]=="仙台市") & ((df_data_times_city["時間軸（調査年）"]=="1980年")|(df_data_times_city["時間軸（調査年）"]=="1985年"))]

        # 千葉を追加 # 千葉市は1995年から区割り（1990年までは千葉市のみ）
        df_chiba = df_data_times_city.loc[(df_data_times_city["市区町村"]=="千葉市") & ((df_data_times_city["時間軸（調査年）"]=="1980年")|(df_data_times_city["時間軸（調査年）"]=="1985年")| \
            (df_data_times_city["時間軸（調査年）"]=="1990年"))]

        # 相模原を追加 # 相模原市は2010年から区割り（2005年までは相模原市のみ)
        df_sagamihara = df_data_times_city.loc[(df_data_times_city["市区町村"]=="相模原市") & ((df_data_times_city["時間軸（調査年）"]=="1980年")|(df_data_times_city["時間軸（調査年）"]=="1985年")| \
            (df_data_times_city["時間軸（調査年）"]=="1990年") |(df_data_times_city["時間軸（調査年）"]=="1995年")|(df_data_times_city["時間軸（調査年）"]=="2000年")|(df_data_times_city["時間軸（調査年）"]=="2005年"))]

        # 新潟市は2010年から区割り（2005年までは新潟市のみ)
        df_niigata = df_data_times_city.loc[(df_data_times_city["市区町村"]=="新潟市") & ((df_data_times_city["時間軸（調査年）"]=="1980年")|(df_data_times_city["時間軸（調査年）"]=="1985年")| \
            (df_data_times_city["時間軸（調査年）"]=="1990年") |(df_data_times_city["時間軸（調査年）"]=="1995年")|(df_data_times_city["時間軸（調査年）"]=="2000年")|(df_data_times_city["時間軸（調査年）"]=="2005年"))]

        # 静岡市は2005年から区割り（2000年までは静岡市のみ)
        df_shizuoka = df_data_times_city.loc[(df_data_times_city["市区町村"]=="静岡市") & ((df_data_times_city["時間軸（調査年）"]=="1980年")|(df_data_times_city["時間軸（調査年）"]=="1985年")| \
            (df_data_times_city["時間軸（調査年）"]=="1990年") |(df_data_times_city["時間軸（調査年）"]=="1995年")|(df_data_times_city["時間軸（調査年）"]=="2000年"))]

        # 浜松市は2010年から区割り（2005年までは浜松市のみ）
        df_hamamatu = df_data_times_city.loc[(df_data_times_city["市区町村"]=="浜松市") & ((df_data_times_city["時間軸（調査年）"]=="1980年")|(df_data_times_city["時間軸（調査年）"]=="1985年")| \
            (df_data_times_city["時間軸（調査年）"]=="1990年") |(df_data_times_city["時間軸（調査年）"]=="1995年")|(df_data_times_city["時間軸（調査年）"]=="2000年")|(df_data_times_city["時間軸（調査年）"]=="2005年"))]

        # 堺市は2010年から区割り(2005年までは堺市）)
        df_sakai = df_data_times_city.loc[(df_data_times_city["市区町村"]=="堺市") & ((df_data_times_city["時間軸（調査年）"]=="1980年")|(df_data_times_city["時間軸（調査年）"]=="1985年")| \
            (df_data_times_city["時間軸（調査年）"]=="1990年") |(df_data_times_city["時間軸（調査年）"]=="1995年")|(df_data_times_city["時間軸（調査年）"]=="2000年")|(df_data_times_city["時間軸（調査年）"]=="2005年"))]

        # 岡山市は2010年から区割り（2005年までは岡山市）
        df_okayama = df_data_times_city.loc[(df_data_times_city["市区町村"]=="岡山市") & ((df_data_times_city["時間軸（調査年）"]=="1980年")|(df_data_times_city["時間軸（調査年）"]=="1985年")| \
            (df_data_times_city["時間軸（調査年）"]=="1990年") |(df_data_times_city["時間軸（調査年）"]=="1995年")|(df_data_times_city["時間軸（調査年）"]=="2000年")|(df_data_times_city["時間軸（調査年）"]=="2005年"))]

        # 熊本は2015年から区割り（2010年までは熊本市）
        df_kumamoto = df_data_times_city.loc[(df_data_times_city["市区町村"]=="熊本市") & ((df_data_times_city["時間軸（調査年）"]=="1980年")|(df_data_times_city["時間軸（調査年）"]=="1985年")| \
            (df_data_times_city["時間軸（調査年）"]=="1990年") |(df_data_times_city["時間軸（調査年）"]=="1995年")|(df_data_times_city["時間軸（調査年）"]=="2000年")| \
                (df_data_times_city["時間軸（調査年）"]=="2005年")|(df_data_times_city["時間軸（調査年）"]=="2010年"))]


        df_data_times_del_city = pd.concat([df_data_times_del_city,df_sendai,df_chiba,df_sagamihara,df_niigata,df_shizuoka,df_hamamatu,df_sakai,df_okayama,df_kumamoto],axis=0)

        # 東京都には特別区フラグと東京都5区フラグをつける。
        # ここから取得:https://1-notes.com/datas-toukyou-area/
        ku23_list = ['千代田区', '中央区', '港区', '新宿区', '文京区', '台東区', '墨田区', '江東区', '品川区', '目黒区', '大田区', \
            '世田谷区', '渋谷区', '中野区', '杉並区', '豊島区', '北区', '荒川区', '板橋区', '練馬区', '足立区', '葛飾区', '江戸川区']
        ku5_list = ['千代田区', '中央区', '港区', '渋谷区', '新宿区']

        df_data_times_del_city["特別区部flag_23"] = df_data_times_del_city["市区町村"].apply(lambda x : 1 if x in ku23_list else 0)
        df_data_times_del_city["特別区部flag_5"] = df_data_times_del_city["市区町村"].apply(lambda x : 1 if x in ku5_list else 0)


        # 特別区部を削除する
        df_data_times_del_tooku = df_data_times_del_city[df_data_times_del_city["市区町村"]!="特別区部"]

        return df_data_times_del_tooku

    def get_times_men_data(self,df_data_times_del_tooku: pd.DataFrame) -> pd.DataFrame:
        """
        get_times_men_data ⇒ (df_data_time_men[DataFrame]): get_times_data_prep関数の出力データフレームに面積のデータをマージする
        

        Args:
            df_data_times_del_tooku[DataFrame]:国勢調査の取得した年のファイルに政令指定都市と特別区の前処理をしたデータフレーム （get_times_data_prep関数の出力値）
        Returns:
            df_data_time_men[DataFrame]:市区町村の面積のデータをmergeしたもの（2020年度のみすべての市区町村がマッピング可能）
        Raises:
            なし
        Examples:
            df_data_time_men = DataPrep.get_times_men_data(df_data_times_del_tooku)
        Note:
            なし
        """

        df_menseki = pd.read_csv(self.DATA_PATH_MENSEKI,encoding = "cp932")[3:]

        # カラム名をつける
        list_col_menseki = [i for i in df_menseki.iloc[0]]
        df_menseki.columns = list_col_menseki
        df_menseki.drop(3,axis=0,inplace = True)


        # 必要な列のみにしぼる※令和4年10月1日のデータのみ使用する
        list_col_menseki_needed = ["標準地域コード" ,"都道府県" , "郡･支庁･振興局等" , "市区町村" , "令和4年10月1日(k㎡)", "令和4年10月1日備考"]
        df_menseki = df_menseki[list_col_menseki_needed]


        #標準コードを数字文字列のものだけに絞る
        df_menseki["n_judge"] = df_menseki["標準地域コード"].apply(lambda x: True if re.fullmatch('[0-9]+',str(x)) else False)
        df_menseki_jd = df_menseki.loc[df_menseki["n_judge"]]


        # ５桁に変換する
        df_menseki_jd["標準地域コード"] = df_menseki_jd["標準地域コード"].apply(lambda x: "0"+ str(x) if len(str(x))<5 else str(x))


        # df_data_times_del_tookuの[area_code]をkeyにmergeする
        df_data_time_men = pd.merge(df_data_times_del_tooku,df_menseki_jd[["標準地域コード","令和4年10月1日(k㎡)"]], how= "left",left_on = "area_code", right_on="標準地域コード")

        # 並び替え
        df_data_time_men = df_data_time_men.sort_values(by=["area_code","時間軸（調査年）"], ascending=[False,True])

        return df_data_time_men



if __name__=="__main__":
    DataPrep = DataPrepPopulation()
    df_data_times = DataPrep.get_times_data()
    df_data_times_del_tooku = DataPrep.get_times_data_prep(df_data_times)
    df_data_time_men = DataPrep.get_times_men_data(df_data_times_del_tooku)
    # Tableau用
    df_data_time_men.to_csv("../2.output_data/data_population_1980to2020.csv",index=False,encoding="utf_8")
    # 目検用
    df_data_time_men.to_csv("../2.output_data/data_population_1980to2020_shift_jis.csv",index=False,encoding="CP932")
