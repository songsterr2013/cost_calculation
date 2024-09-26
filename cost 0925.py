import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import timedelta
import numpy as np
import time


# 連線資料庫
def connect_to_mysql(task, database, start=None, end=None):  # 參數:(工序, 哪一個TABLE?,開始時間跟結束時間*可選)

    try:
        cnx = mysql.connector.connect(host='',
                                      user='',
                                      passwd='',
                                      )
        if cnx.is_connected():
            # check the version of the db
            db_Info = cnx.get_server_info()
            cursor = cnx.cursor(named_tuple=True)
            print('db_version:', db_Info)

    except Error as e:
        print("資料庫連接失敗：", e)

    if task == 'laser':
        if database == 'svfs':
            cursor.execute("SELECT machine, program_name, start_datetime, end_datetime, TIMEDIFF(end_datetime,start_datetime) as `delta_time`, results_count, bad_count \
                            FROM svfs.sheets_log \
                            WHERE start_datetime >= '{}' and end_datetime <='{}' ORDER BY start_datetime ASC".format(
                start, end))
            sheets_log = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the laser data !')
            return sheets_log

        elif database == 'qrcode_label':
            cursor.execute("SELECT program_id, metal_no, amount \
                            FROM qrcode_label.metal_no_list ")
            data_qrcode = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the metal_no_list data !')
            return data_qrcode

        elif database == 'smbsource':
            cursor.execute("SELECT product_id,parent_id,amount \
                            FROM smbsource.bom \
                            WHERE mp_id = 'MB' ")
            parent = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the bom data !')
            return parent

    elif task == 'bending':
        if database == 'svfs':
            cursor.execute("SELECT machine , part_name , result_start_time , result_end_time , TIMEDIFF(result_end_time,result_start_time) as `delta_time` , result_count , result_badcount \
                            FROM svfs.parts_log\
                            WHERE result_start_time >= '{}' and result_end_time <='{}' ORDER BY result_start_time ASC".format(start, end))
            parts_log = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the bending data !')
            return parts_log

        elif database == 'smbsource':
            cursor.execute("SELECT product_id,parent_id,amount \
                            FROM smbsource.bom\
                            WHERE mp_id = 'MC' ")
            parent = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the bom data !')
            return parent

    elif task == 'welding':
        if database == 'welding_report':
            cursor.execute("SELECT worker_id, parent_id, result_start_time, result_end_time, TIMEDIFF(result_end_time,result_start_time) as `delta_time`,expect_amount, result_amount\
                            FROM welding_report.welding_log\
                            WHERE result_end_time >= '{}' and result_end_time <='{}' ORDER BY result_start_time ASC".format(start, end))
            welding_log = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the welding data !')
            return welding_log

        elif database == 'welding_report2':  # 這邊只好叫它2了，因為它剛好跟上面撞同一個DATABASE
            cursor.execute("SELECT username , realname \
                            FROM welding_report.weldworker")
            data_name = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the worker name data !')
            return data_name

    elif task == 'bom':
        if database == 'bom':
            cursor.execute("SELECT *\
                            FROM smbsource.bom")
            bom_data = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the bom data !')
            return bom_data


# 雷射/折床/焊接，幫加上週數及週總耗時
# 參數:(工序, data=前面資料庫拿出來的DATA)
def pre_processing(task, data):
    if task == 'laser':
        week_number = 1  # 從哪一週開始
        count_time = timedelta()  # 耗時加總用
        index_list = []  # 把這一週的東西新增進去
        real_sheets_log = []

        for number, index in enumerate(data):
            if week_number == int(index.start_datetime.strftime("%U")) + 1:  # 第一次進的會是23
                count_time += index[4]  # 裡面的所有耗時加總
                index_list.append(index)  # 把屬於23週的東西新增到List

            elif week_number != int(index.start_datetime.strftime("%U")) + 1:  # 當它不再=23，應該說當他換週時:
                for index_save in index_list:  # 剛剛新增進去list裡面的東西
                    real_sheets_log.append([index_save[0],
                                            index_save[1],
                                            int(index_save.start_datetime.strftime("%U")) + 1,
                                            count_time,
                                            index_save[2],
                                            index_save[3],
                                            index_save[4],
                                            index_save[5],
                                            index_save[6],
                                            ])  # list裡面的東西

                count_time = timedelta()  # 重畳
                index_list = []  # 重畳
                week_number = int(index.start_datetime.strftime("%U")) + 1  # 因為跳到下一週了，它本身會進入elif，這邊的週數也變成了那一週
                print('i have already reset and the new laser week_number is:', week_number)
                count_time += index[4]  # 裡面的所有耗時加總
                index_list.append(index)  # 把屬於下一週的東西新增到List

            if number == len(data) - 1:  # 當它來到最後一行時
                print('Here is the laser last line')
                for index_save in index_list:
                    real_sheets_log.append([index_save[0],
                                            index_save[1],
                                            int(index_save.start_datetime.strftime("%U")) + 1,
                                            count_time,
                                            index_save[2],
                                            index_save[3],
                                            index_save[4],
                                            index_save[5],
                                            index_save[6],
                                            ])  # list裡面的東西
        return real_sheets_log

    elif task == 'bending' or 'welding':  # 折床跟焊接進同一個

        week_number = 1  # 從哪一週開始
        count_time = timedelta()  # 耗時加總用
        index_list = []  # 把這一週的東西新增進去
        real_log = []

        for number, index in enumerate(data):
            if week_number == int(index.result_start_time.strftime("%U")) + 1:  # 第一次進的會是23
                count_time += index[4]  # 裡面的所有耗時加總
                index_list.append(index)  # 把屬於23週的東西新增到List

            elif week_number != int(index.result_start_time.strftime("%U")) + 1:  # 當它不再=23，應該說當他換週時:
                for index_save in index_list:  # 剛剛新增進去list裡面的東西
                    real_log.append([index_save[0],
                                     index_save[1],
                                     int(index_save.result_start_time.strftime("%U")) + 1,
                                     count_time,
                                     index_save[2],
                                     index_save[3],
                                     index_save[4],
                                     index_save[5],
                                     index_save[6],
                                     ])  # list裡面的東西

                count_time = timedelta()  # 重畳
                index_list = []  # 重畳
                week_number = int(index.result_start_time.strftime("%U")) + 1  # 因為跳到下一週了，它本身會進入elif，這邊的週數也變成了那一週
                print('i have already reset and the bending/welding new week_number is:', week_number)
                count_time += index[4]  # 裡面的所有耗時加總
                index_list.append(index)  # 把屬於下一週的東西新增到List

            if number == len(data) - 1:  # 當它來到最後一行時
                print('Here is the bending/welding last line')
                for index_save in index_list:
                    real_log.append([index_save[0],
                                     index_save[1],
                                     int(index_save.result_start_time.strftime("%U")) + 1,
                                     count_time,
                                     index_save[2],
                                     index_save[3],
                                     index_save[4],
                                     index_save[5],
                                     index_save[6],
                                     ])  # list裡面的東西
        return real_log


# 雷射，前處理後的雷射DATA加上子件料號跟數量,母件名稱
# 參數:(前處理後的雷射DATA, 排版圖DATA, BOM表)
def merge_from_typesetting_and_bom(data, data_qrcode, parent):
    # 缺子件名稱的雷射報表
    df_sheets_log = pd.DataFrame(data)
    df_sheets_log.columns = ['機台', '排版圖編號', '週數', '週總耗時', '起始時間', '結束時間', '耗時', '完成數量', '失敗數量']
    # 從排版圖去撈母件名稱
    df_components = pd.DataFrame(data_qrcode)
    df_components.columns = ['排版圖編號', '子件料號', '子件數量']
    # merge完子件料號跟數量
    data = pd.merge(df_sheets_log, df_components, how='left')
    # 從bom表去拿母件料號
    df_parents = pd.DataFrame(parent)
    df_parents.columns = ['子件料號', '母件料號', '需求數量']
    data_merged = pd.merge(data, df_parents, how='left')
    return data_merged


# 折床，前處理後的折床DATA加上母件料號跟數量
# 參數:(前處理後的折床DATA,BOM表)
def merge_from_bom(data, parent):
    # 缺母件名稱的折床報表
    df_parts_log = pd.DataFrame(data)
    df_parts_log.columns = ['機台', '子件料號', '週數', '週總耗時', '起始時間', '結束時間', '耗時', '完成數量', '失敗數量']
    # 從bom去撈母件名稱
    df_parent = pd.DataFrame(parent)
    df_parent.columns = ['子件料號', '母件料號', '需求數量']
    data_merged = pd.merge(df_parts_log, df_parent, how='left')
    # 合起來，有些顯示NAN，是因為這個子件根本就沒有折床工序卻莫名奇妙地出現了，
    # 有的是新子件
    return data_merged


# 焊接，前處理後的焊接DATA加上工人名稱
# 參數:(前處理後的焊接DATA, 工人名稱)
def merge_from_worker_name(data, name):
    # 缺工人名稱的焊接報表
    df_parents_log = pd.DataFrame(data)
    df_parents_log.columns = ['工人編號', '母件料號', '週數', '週總耗時', '起始時間', '結束時間', '耗時', '預期完成數量', '實際生產數量']
    # 從weld_worker去撈工人名稱
    df_name = pd.DataFrame(name)
    df_name.columns = ['工人編號', '工人']
    data_merged = pd.merge(df_parents_log, df_name, how='left')
    # 合起來
    return data_merged


# 焊接，為焊接DATA加上標準工時
# 這邊的return比較特別，等之後焊接data都正常時才能改掉
# 參數:(要跟標準工時接在一起的焊接DATA)
def merge_from_welding_standard_time(data):
    # 讀取標準工時表
    standard_time_table = pd.read_csv('焊接標準工時.csv')
    standard_time_table = standard_time_table[['product_no', 'time']]  # 只拿我要的COLUMNS
    standard_time_table.columns = ['母件料號', '標準工時']
    # 合起來
    data_merged_2 = pd.merge(data, standard_time_table, how='left')
    return data_merged_2[data_merged_2['實際生產數量'] >= 1]  # 有些實際生產是0的是工人操作不當或我們收data的問題，它或許是一個真實的數據，但錯的太誇張了，一個母件焊22天???


# 焊接，把剛剛的DATA從DATAFRAME轉回LIST操作，再去算它的標準工時週總耗時
# 參數:(純粹把DATA丟進來而已)
def extra_processing_for_welding_data(data):
    data_list = []
    data_transition = np.array(data)
    data_transition = data_transition.tolist()

    for i in data_transition:
        if type(i[10]) == str:
            data_list.append([i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], int(i[10]), i[8]*int(i[10])])
        else:
            data_list.append([i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], 0, 0])  # 把NAN值直接變0，不然後面根本運算不了

    week_number = 1  # 從哪一周開始算
    count_time = 0  # 耗時加總用
    index_list = []  # 把這一週的東西新增進去
    real_data = []

    for number, index in enumerate(data_list):
        if week_number == index[2]:  # 第一次進的會是23
            count_time += index[11]  # 裡面的所有耗時加總
            index_list.append(index)  # 把屬於23週的東西新增到List

        elif week_number != index[2]:  # 當它不再=23，應該說當他換週時:
            for index_save in index_list:  # 剛剛新增進去list裡面的東西
                real_data.append([index_save[0],
                                  index_save[1],
                                  index_save[2],
                                  index_save[3],
                                  index_save[4],
                                  index_save[5],
                                  index_save[6],
                                  index_save[7],
                                  index_save[8],
                                  index_save[9],
                                  index_save[10],
                                  index_save[11],
                                  count_time
                                  ])  # list裡面的東西

            count_time = 0  # 重畳
            index_list = []  # 重畳
            week_number = index[2]  # 因為跳到下一週了，它本身會進入elif，這邊的週數也變成了那一週
            count_time += index[11]  # 裡面的所有耗時加總
            index_list.append(index)  # 把屬於下一週的東西新增到List

        if number == len(data_list) - 1:  # 當它來到最後一行時
            for index_save in index_list:
                real_data.append([index_save[0],
                                  index_save[1],
                                  index_save[2],
                                  index_save[3],
                                  index_save[4],
                                  index_save[5],
                                  index_save[6],
                                  index_save[7],
                                  index_save[8],
                                  index_save[9],
                                  index_save[10],
                                  index_save[11],
                                  count_time
                                  ])  # list裡面的東西

    the_real_data = pd.DataFrame(real_data)
    the_real_data.columns = ['工人編號', '母件料號', '週數', '週總耗時', '起始時間', '結束時間', '耗時', '預期完成數量', '實際生產數量', '工人', '標準工時',
                             '乘上數量後的標準工時', '週總標準工時耗時']
    return the_real_data


# 雷射/折床，處理子件數量為0及有母件料號的異常值()，因為資料庫的排版圖編號、BOM 不完整，所以母件料號需要額外的規則去產生
# 參數:(工序, 雷射DATA/折床DATA)
def deal_with_abnormal(task, real_data):

    if task == 'laser':
        for index, row in real_data.iterrows():
            if row[10] >= 1:  # 處理子件數量為0的時候，把它通通變1，他的邏輯就會變成是他有分攤到耗時
                pass
            else:
                real_data.iloc[index, 10] = 1

            if type(row[11]) != str:  # 處理沒有母件料號的子件，用-分開，然後取第0個或=子件料號
                if type(row[9]) == str:
                    if len(row[9].split('-')) >= 2:  # 不是為1的話他只有小部份例外，他比較長的字串在中間，不要管他，反正都取第0個
                        parent = [index, row[9].split('-')[0]]
                        real_data.iloc[parent[0], 11] = parent[1]  # '*' +
                    else:  # 就都取自己就對了
                        parent = [index, row[9]]
                        real_data.iloc[parent[0], 11] = parent[1]  # '*' +
        return real_data

    elif task == 'bending':
        for index, row in real_data.iterrows():
            if type(row[9]) == float:
                if len(row[1].split('-')) >= 2:
                    parent = [index, row[1].split('-')[0]]
                    real_data.iloc[parent[0], 9] = parent[1]  # '*' +
                else:
                    parent = [index, row[1]]
                    real_data.iloc[parent[0], 9] = parent[1]  # '*' +
            if row[7] == 0:  # 這個地方處理的是'如果完成數量為0',前面用的「數量」就代表它的完成數量，如果也是0的話就只能是1了
                if row[10] >= 1:
                    real_data.iloc[index, 7] = row[10]
                else:
                    real_data.iloc[index, 7] = 1
        return real_data


# 雷射，把排版圖中的子件總數併到雷射DATA，並處理異常值
# 參數:(排版圖中的子件總數,原生雷射DATA)
def amount_of_typesetting(data_qrcode, real_data):
    df_components = pd.DataFrame(data_qrcode)
    df_components.columns = ['排版圖編號', '子件料號', '子件數量']
    agg = df_components.groupby('排版圖編號')['子件數量'].agg(np.sum)
    amounts = pd.DataFrame(agg)
    amounts.reset_index(inplace=True)
    amounts.columns = ['排版圖編號', '排版圖子件數']
    real_data_no2 = pd.merge(real_data, amounts, how='left')

    for index, row in real_data_no2.iterrows():
        if row[13] >= 1:  # 處理排版圖子件數為0的時候
            pass
        else:
            real_data_no2.iloc[index, 13] = 1
    return real_data_no2


# 共用，加上薪資表欄位，把薪資攤到子件/母件上
# 參數:(工序, 基本處理完成後的各個DATA)
def get_wage_table(task, data):
    wage_table = pd.read_excel('-歷史薪水.xlsx')  # 讀取薪資表
    if task == 'typesetting':  # 排版
        agg = wage_table.groupby('週數')['排版薪資'].agg(np.sum)
        real_wage_table = pd.DataFrame(agg)
        real_wage_table.reset_index(inplace=True)
        real_data_no3 = pd.merge(data, real_wage_table, how='left')
        return real_data_no3
    if task == 'laser':  # 雷射
        agg = wage_table.groupby('週數')['雷射薪資'].agg(np.sum)
        real_wage_table = pd.DataFrame(agg)
        real_wage_table.reset_index(inplace=True)
        real_data_no3 = pd.merge(data, real_wage_table, how='left')
        return real_data_no3
    elif task == 'bending':  # 折床
        agg = wage_table.groupby('週數')['折床薪資'].agg(np.sum)
        real_wage_table = pd.DataFrame(agg)
        real_wage_table.reset_index(inplace=True)
        real_data_no3 = pd.merge(data, real_wage_table, how='left')
        return real_data_no3
    elif task == 'welding':  # 焊接
        agg = wage_table.groupby('週數')['焊接薪資'].agg(np.sum)
        real_wage_table = pd.DataFrame(agg)
        real_wage_table.reset_index(inplace=True)
        real_data_no3 = pd.merge(data, real_wage_table, how='left')
        return real_data_no3


# 雷射/折床/焊接，算分攤耗時跟分攤成本
# 參數:(工序，處理完後的各個DATA)
def divided_consumption_cost(task, data, welding_type=None):
    if task == 'laser':
        divided_time = []
        divided_cost = []
        for index, row in data.iterrows():
            divided_time.append(row[6] * row[10] / row[13])
        data['分攤耗時'] = divided_time
        for index, row in data.iterrows():
            divided_cost.append(row[14] * row[15] / row[3])
        data['分攤成本'] = divided_cost
        # 處理排版圖子件數為0的時候
        for index, row in data.iterrows():
            if type(row[9]) != str:
                data.iloc[index, 9] = 'None'
                data.iloc[index, 11] = 'None'
        return data

    elif task == 'bending':
        divided_cost = []
        for index, row in data.iterrows():
            divided_cost.append(row[11] * row[6] / row[3])
        data['總成本'] = divided_cost
        return data

    elif task == 'welding':
        # 如果是算週的東西就來這邊，因為焊接取DATA的規則一律取結束時間，然後它的DATA異常又很多，很常有一個母件焊了1週以上的時間(跨週)
        # 這樣一週的薪資會攤在1-2個母件上，非常不合理，所以這邊的處理方法是，把它當作同一週的東西，只用一週的薪資去攤它
        def get_the_cost(x):
            return x['焊接薪資'] * x['乘上數量後的標準工時'] / x['週總標準工時耗時']

        if welding_type == 'week':
            unified_wage = int(data.iloc[-1:, 12])
            only_one_week = data.iloc[-1:, 2]
            for q, i in enumerate(data['週總標準工時耗時']):  # 週總耗時重新加總
                data.iloc[q, 12] = sum(data['乘上數量後的標準工時'])
            for q, i in enumerate(data['焊接薪資']):  # 薪資就取我要看的最新那週
                data.iloc[q, 13] = unified_wage
            for q, i in enumerate(data['週數']):  # 考慮到電費那邊的計算，把這邊的週也變成同一週的
                data.iloc[q, 2] = only_one_week
            data['總成本'] = data.apply(lambda x: get_the_cost(x), axis=1)
            return data

        else:
            data['總成本'] = data.apply(lambda x: get_the_cost(x), axis=1)
            return data


# 排版/雷射/折床/焊接，最後算出單一子件成本
# 參數:(工序，處理完後的各個DATA,組成一個母子的需要不同的子件，不同的子件所需數量不只1個，所以這邊加上amount去撈它的母件需求數量,決定它處理的是前一個說的哪一種)
def cost_per_one(task, data, amount=None, single_or_multiple=None):

    if task == 'typesetting':
        true_typesetting_cost = []
        grouped = data.groupby(['子件料號', '母件料號'])  # 用 groupby的方法處理
        agg = grouped['子件數量', '分攤成本'].agg(np.sum)
        typesetting_cost = pd.DataFrame(agg)
        typesetting_cost.reset_index(inplace=True)
        for i, q in zip(typesetting_cost['分攤成本'], typesetting_cost['子件數量']):
            true_typesetting_cost.append(i / q)
        typesetting_cost['子件排版單一成本'] = true_typesetting_cost

        if single_or_multiple == 'single':  # ****這邊return的就是單一成本而已，下面的則是已經考慮到母件的子件需求數量****
            return typesetting_cost

        elif single_or_multiple == 'multiple':  # ****下面這邊則是處理子件依據母件需求合起來後的子件成本****
            df_parents = pd.DataFrame(amount)
            df_parents.columns = ['子件料號', '母件料號', '需求數量']
            typesetting_cost_2 = pd.merge(typesetting_cost, df_parents, how='left')

            def sum_up_the_component(x):
                if x['需求數量'] > 0:
                    return x['子件排版單一成本'] * x['需求數量']
                else:
                    return x['子件排版單一成本']

            typesetting_cost_2['子件排版單一成本'] = typesetting_cost_2.apply(lambda x: sum_up_the_component(x), axis=1)
            return typesetting_cost_2

    elif task == 'laser':
        true_laser_cost = []
        grouped = data.groupby(['子件料號', '母件料號'])  # 用 groupby的方法處理
        agg = grouped['子件數量', '分攤成本'].agg(np.sum)
        laser_cost = pd.DataFrame(agg)
        laser_cost.reset_index(inplace=True)
        for i, q in zip(laser_cost['分攤成本'], laser_cost['子件數量']):
            true_laser_cost.append(i / q)
        laser_cost['子件雷射單一成本'] = true_laser_cost

        if single_or_multiple == 'single':  # ****這邊return的就是單一成本而已，下面的則是已經考慮到母件的子件需求數量****
            return laser_cost

        elif single_or_multiple == 'multiple':  # ****下面這邊則是處理子件依據母件需求合起來後的子件成本****
            # 下面這邊則是處理子件依據母件需求合起來後的子件成本
            df_parents = pd.DataFrame(amount)
            df_parents.columns = ['子件料號', '母件料號', '需求數量']
            laser_cost_2 = pd.merge(laser_cost, df_parents, how='left')

            def sum_up_the_component(x):
                if x['需求數量'] > 0:
                    return x['子件雷射單一成本'] * x['需求數量']
                else:
                    return x['子件雷射單一成本']

            laser_cost_2['子件雷射單一成本'] = laser_cost_2.apply(lambda x: sum_up_the_component(x), axis=1)
            return laser_cost_2

    elif task == 'bending':
        true_bending_cost = []
        grouped = data.groupby(['子件料號', '母件料號'])  # 用 groupby的方法處理
        agg = grouped['完成數量', '總成本'].agg(np.sum)
        bending_cost = pd.DataFrame(agg)
        bending_cost.reset_index(inplace=True)
        for i, q in zip(bending_cost['總成本'], bending_cost['完成數量']):
            true_bending_cost.append(i / q)
        bending_cost['子件折床單一成本'] = true_bending_cost

        if single_or_multiple == 'single':  # ****這邊return的就是單一成本而已，下面的則是已經考慮到母件的子件需求數量****
            return bending_cost

        elif single_or_multiple == 'multiple':  # ****下面這邊則是處理子件依據母件需求合起來後的子件成本****
            df_parents = pd.DataFrame(amount)
            df_parents.columns = ['子件料號', '母件料號', '需求數量']
            bending_cost_2 = pd.merge(bending_cost, df_parents, how='left')

            def sum_up_the_component(x):
                if x['需求數量'] > 0:
                    return x['子件折床單一成本'] * x['需求數量']
                else:
                    return x['子件折床單一成本']

            bending_cost_2['子件折床單一成本'] = bending_cost_2.apply(lambda x: sum_up_the_component(x), axis=1)
            return bending_cost_2

    elif task == 'welding':  # 它本身就是母件了
        true_welding_cost = []
        grouped = data.groupby('母件料號')  # 用 groupby的方法處理
        agg = grouped['實際生產數量', '總成本'].agg(np.sum)
        welding_cost = pd.DataFrame(agg)
        welding_cost.reset_index(inplace=True)
        for i, q in zip(welding_cost['總成本'], welding_cost['實際生產數量']):
            true_welding_cost.append(i / q)
        welding_cost['母件焊接單一成本'] = true_welding_cost
        return welding_cost


# 共用，最後匯出成單一子件的EXCEL檔的話都是來這一步
# 參數:(DATA,命名)
def export_to_excel(data, rename):
    data.to_excel('{}.xlsx'.format(rename), index=False, header=True, encoding='utf_8_sig')
# ====================================================以上大多為工錢的CODE====================================================


# 拉會計給我的BOM，跟從進貨表中整理好的進貨成本，去算出料錢，之後怎麼樣一直收DATA就要大改這邊了
def get_material_data_and_bom():

    material_hardware = pd.read_excel('-0908最新會計統整後的原料成本.xlsx', sheet_name=1)  # 會計整理過的五金件成本
    material_hardware = material_hardware[['零件名稱', '進價']]  # 只拿我要的欄位
    material_hardware.columns = ['零件名稱', '五金進價']  # 改名等下MERGE用
    print('Successfully got material_data 1 ! (1/5)')

    material_iron = pd.read_excel('-0908最新會計統整後的原料成本.xlsx', sheet_name=3)  # 會計整理過的鈑材類成本
    material_iron = material_iron[['類別', '平均單價']]  # 只拿我要的欄位
    material_iron.columns = ['產品類別', '鈑材平均單價']  # 改名等下MERGE用
    print('Successfully got material_data 2 ! (2/5)')

    bom = pd.read_excel('-最新BOM表.xlsx', sheet_name=1)  # 會計整理過後的BOM表，真實的BOM有60000多筆...要怎麼拿?
    bom_df = bom[
        ['母件編號', '母件名稱', '母件類別', '項次編號', '零件名稱', '版本', '規格', '數量', '重量', '面積', '產品類別', '排版', '雷射', '折床', '焊接', '總重',
         '總面積']]  # 只拿我要的欄位
    print('Successfully got bom ! (3/5)')

    bom_df_with_cost = pd.merge(bom_df, material_hardware, how='left')  # BOM MERGE 五金件成本
    bom_df_with_cost2 = pd.merge(bom_df_with_cost, material_iron, how='left')  # BOM MERGE 鈑材類成本
    bom_df_with_cost2 = bom_df_with_cost2.drop_duplicates(subset=None, keep='first',
                                                          inplace=False)  # 去除重覆值，因為五金件有些名字重覆，導致會生出重覆DATA
    print('Successfully merged ! (4/5)')

    def count_the_cost(x):
        if x['產品類別'] in ['F01', 'F03', 'F05']:
            return x['五金進價'] * x['數量']
        else:
            return x['鈑材平均單價'] * x['總重']

    bom_df_with_cost2['原料成本'] = bom_df_with_cost2.apply(lambda x: count_the_cost(x), axis=1)

    grouped = bom_df_with_cost2.groupby(['母件編號', '母件類別'])  # 用groupby的方法處理
    agg = grouped['原料成本'].agg(np.sum)
    cost = pd.DataFrame(agg)
    cost.reset_index(inplace=True)
    cost.columns = ['母件料號', '母件類別', '原料成本']
    print('Successfully reorganized ! (5/5)')
    return cost
# ====================================================以上為計算料錢的CODE====================================================


# #雷射/折床/焊接，把前面的各個報表拉進來做處理，幫它們加上加總後的週總耗時，以及計算它們的分攤耗電
# 參數:(雷射，折床，焊接)
def concat_data_with_electricity(las, bend, weld):

    electricity_laser = las[['週數', '母件料號', '子件料號', '子件數量', '分攤耗時']]
    electricity_laser.loc[:, '種類'] = 'laser'  # 幫它加上種類
    electricity_laser.columns = ['週數', '母件料號', '子件料號', '完成數量', '耗時(分鐘)', '種類']
    electricity_laser.loc[:, '耗時(分鐘)'] = electricity_laser['耗時(分鐘)'].dt.seconds/60  # 如果不把它從timedelta轉到int，等下會沒法算

    electricity_bending = bend[['週數', '母件料號', '子件料號', '完成數量', '耗時']]
    electricity_bending.loc[:, '種類'] = 'bending'  # 幫它加上種類
    electricity_bending.columns = ['週數', '母件料號', '子件料號', '完成數量', '耗時(分鐘)', '種類']
    electricity_bending.loc[:, '耗時(分鐘)'] = electricity_bending['耗時(分鐘)'].dt.seconds/60  # 如果不把它從timedelta轉到int，等下會沒法算

    electricity_welding = weld[['週數', '母件料號', '母件料號', '實際生產數量', '乘上數量後的標準工時']]
    electricity_welding.loc[:, '種類'] = 'welding'  # 幫它加上種類
    electricity_welding.columns = ['週數', '母件料號', '子件料號', '完成數量', '耗時(分鐘)', '種類']

    data_for_electricity = pd.concat([electricity_laser, electricity_bending, electricity_welding],
                                     axis=0, ignore_index=True)

    grouped = data_for_electricity.groupby('週數')
    agg = grouped['耗時(分鐘)'].agg(np.sum)
    consumption_of_week = pd.DataFrame(agg)
    consumption_of_week.reset_index(inplace=True)
    consumption_of_week.columns = ['週數', '週總耗時(分鐘)']
    data_for_electricity_with_week = pd.merge(data_for_electricity, consumption_of_week, how='left')
    return data_for_electricity_with_week


# 雷射/折床/焊接，分攤電費成本，這邊的consumption有做一個+1的處理，也就是說我想看23週的，他就會去對24那邊，算出23-24之間用了多少電
# 參數:(把CONCAT後的DATA丟進去它就會知道它是第幾週，哪個廠了)
def get_table_and_calculation(data):
    # 讀取電費表
    electricity_bill = pd.read_excel('-歷史電費.xlsx')
    electricity_bill = electricity_bill.iloc[1:, 0:]
    agg = electricity_bill.groupby('week')[
        'consuming_1', 'consuming_2', 'consuming_3', 'consuming_4', 'consuming_5', 'consuming_6', 'consuming_7'].agg(
        np.sum)
    electricity = pd.DataFrame(agg)
    electricity.reset_index(inplace=True)

    a_bunch_of_week = [i for i in data['週數'].unique()]  # 根據我的DATA去撈出我要的電度，再進行計算
    factory_one_or_two = len([i for i in data['種類'].unique()])  # 根據我的DATA去判斷它是1廠的算法還是2廠的算法
    bill = []

    for week in a_bunch_of_week:
        if factory_one_or_two == 3:
            consumption1 = [row[1] for index, row in electricity.iterrows() if row[0] == week + 1][0]
            consumption2 = [row[2] for index, row in electricity.iterrows() if row[0] == week + 1][0]
            consumption3 = [row[3] for index, row in electricity.iterrows() if row[0] == week + 1][0]
            consumption4 = [row[4] for index, row in electricity.iterrows() if row[0] == week + 1][0]

            # 1
            magnification1 = 40  # only for this one
            rate1 = 5.05  # always get the highest one
            cost1 = (consumption1 * magnification1 * rate1) / 1.05  # without tax

            # 2
            magnification2 = 40  # only for this one
            rate2 = 5.05  # always get the highest one
            cost2 = (consumption2 * magnification2 * rate2) / 1.05  # without tax

            # 3
            if week in [row[0] for index, row in electricity.iterrows() if int(row[0]) >= 23 <= 40]:
                agreed_rate3 = (99 * 236.2) * 7 / 29  # summer rate
            else:
                agreed_rate3 = (99 * 173.2) * 7 / 29  # non-summer rate
            magnification3 = 80  # only for this one
            rate3 = 3.33  # always get the highest one
            cost3 = ((consumption3 * magnification3 * rate3) + agreed_rate3) / 1.05  # without tax

            # 4
            if week in [row[0] for index, row in electricity.iterrows() if int(row[0]) >= 23 <= 40]:
                agreed_rate4 = (99 * 236.2) * 7 / 34  # summer rate
            else:
                agreed_rate4 = (99 * 173.2) * 7 / 34  # non-summer rate
            magnification4 = 80  # only for this one
            rate4 = 2.58  # fixed rate
            cost4 = ((consumption4 * magnification4 * rate4) + agreed_rate4) / 1.05  # without tax
            bill.append([week, cost1 + cost2 + cost3 + cost4])

        elif factory_one_or_two == 1:
            consumption5 = [row[5] for index, row in electricity.iterrows() if row[0] == week + 1][0]
            consumption6 = [row[6] for index, row in electricity.iterrows() if row[0] == week + 1][0]
            consumption7 = [row[7] for index, row in electricity.iterrows() if row[0] == week + 1][0]

            # 5
            magnification5 = 1  # only for this one
            rate5 = 5.05  # always get the highest one
            cost5 = (consumption5 * magnification5 * rate5) / 1.05  # without tax

            # 6
            if week in [row[0] for index, row in electricity.iterrows() if int(row[0]) >= 23 <= 40]:
                agreed_rate6 = (14 * 236.2) * 7 / 35  # summer rate
            else:
                agreed_rate6 = (14 * 173.2) * 7 / 35  # non-summer rate
            magnification6 = 1  # only for this one
            rate6 = 2.58  # fixed rate
            cost6 = ((consumption6 * magnification6 * rate6) + agreed_rate6) / 1.05  # without tax

            # 7
            if week in [row[0] for index, row in electricity.iterrows() if int(row[0]) >= 23 <= 40]:
                agreed_rate7 = (99 * 236.2) * 7 / 35  # summer rate
            else:
                agreed_rate7 = (99 * 173.2) * 7 / 35  # non-summer rate
            magnification7 = 80  # only for this one
            rate7 = 2.58  # fixed rate
            cost7 = ((consumption7 * magnification7 * rate7) + agreed_rate7) / 1.05  # without tax
            bill.append([week, cost5 + cost6 + cost7])

    electricity_bill_table = pd.DataFrame(bill)
    electricity_bill_table.columns = ['週數', '電費']
    almost_completed_electricity_cost = pd.merge(data, electricity_bill_table, how='left')

    def electricity_divided_consumption_cost(x):
        return x['電費'] * x['耗時(分鐘)'] / x['週總耗時(分鐘)']

    almost_completed_electricity_cost['分攤電費'] = almost_completed_electricity_cost.apply(
                                                   lambda x: electricity_divided_consumption_cost(x), axis=1)

    return almost_completed_electricity_cost


# 雷射/折床/焊接，最後算出單一子件電費成本
# 參數:(工序，處理完後的各個DATA,組成一個母子的需要不同的子件，不同的子件所需數量不只1個，所以這邊加上amount去撈它的母件需求數量,決定它處理的是前一個說的哪一種)
def electricity_cost_per_one(task, data, amount=None, single_or_multiple=None):

    if task == 'laser':
        true_laser_electricity = []
        laser_electricity = data[data['種類'] == 'laser']

        grouped = laser_electricity.groupby(['子件料號', '母件料號'])  # 用 groupby的方法處理
        agg = grouped['完成數量', '分攤電費'].agg(np.sum)
        laser_electricity_cost = pd.DataFrame(agg)
        laser_electricity_cost.reset_index(inplace=True)
        for i, q in zip(laser_electricity_cost['分攤電費'], laser_electricity_cost['完成數量']):
            true_laser_electricity.append(i / q)
        laser_electricity_cost['子件雷射單一電費成本'] = true_laser_electricity
        if single_or_multiple == 'single':  # ****這邊return的就是單一成本而已，下面的則是已經考慮到母件的子件需求數量****
            return laser_electricity_cost
        elif single_or_multiple == 'multiple':  # ****下面這邊則是處理子件依據母件需求合起來後的子件成本****
            # 下面這邊則是處理子件依據母件需求合起來後的子件成本
            df_parents = pd.DataFrame(amount)
            df_parents.columns = ['子件料號', '母件料號', '需求數量']
            laser_electricity_cost_2 = pd.merge(laser_electricity_cost, df_parents, how='left')

            def sum_up_the_component(x):
                if x['需求數量'] > 0:
                    return x['子件雷射單一成本'] * x['需求數量']
                else:
                    return x['子件雷射單一成本']

            laser_electricity_cost_2['子件雷射單一成本'] = laser_electricity_cost_2.apply(lambda x: sum_up_the_component(x),
                                                                                  axis=1)
            return laser_electricity_cost_2

    if task == 'bending':
        true_bending_electricity = []
        bending_electricity = data[data['種類'] == 'bending']

        grouped = bending_electricity.groupby(['子件料號', '母件料號'])  # 用 groupby的方法處理
        agg = grouped['完成數量', '分攤電費'].agg(np.sum)
        bending_electricity_cost = pd.DataFrame(agg)
        bending_electricity_cost.reset_index(inplace=True)
        for i, q in zip(bending_electricity_cost['分攤電費'], bending_electricity_cost['完成數量']):
            true_bending_electricity.append(i / q)
        bending_electricity_cost['子件雷射單一電費成本'] = true_bending_electricity
        if single_or_multiple == 'single':  # ****這邊return的就是單一成本而已，下面的則是已經考慮到母件的子件需求數量****
            return bending_electricity_cost
        elif single_or_multiple == 'multiple':  # ****下面這邊則是處理子件依據母件需求合起來後的子件成本****
            df_parents = pd.DataFrame(amount)
            df_parents.columns = ['子件料號', '母件料號', '需求數量']
            bending_electricity_cost_2 = pd.merge(bending_electricity_cost, df_parents, how='left')

            def sum_up_the_component(x):
                if x['需求數量'] > 0:
                    return x['子件折床單一成本'] * x['需求數量']
                else:
                    return x['子件折床單一成本']

            bending_electricity_cost_2['子件折床單一成本'] = bending_electricity_cost_2.apply(lambda x: sum_up_the_component(x),
                                                                                      axis=1)
            return bending_electricity_cost_2

    if task == 'welding':
        true_welding_electricity = []
        welding_electricity = data[data['種類'] == 'welding']

        grouped = welding_electricity.groupby(['子件料號', '母件料號'])  # 用 groupby的方法處理
        agg = grouped['完成數量', '分攤電費'].agg(np.sum)
        welding_electricity_cost = pd.DataFrame(agg)
        welding_electricity_cost.reset_index(inplace=True)
        for i, q in zip(welding_electricity_cost['分攤電費'], welding_electricity_cost['完成數量']):
            true_welding_electricity.append(i / q)
        welding_electricity_cost['子件雷射單一電費成本'] = true_welding_electricity
        return welding_electricity_cost
# ====================================================以上為計算電費的CODE====================================================


# 偵測缺的BOM()，在BOM不齊全的情況下，以後每週日都要先丟這個給會計，補上缺的BOM才有辦法讓合併報表變正確
# 參數:(BOM表,攤完成本後的雷射,攤完成本後的折床,攤完成本後的焊接)
def parent_absence_detection(bom, laser_abs, bending_abs, welding_abs=None):
    laser_absence = pd.merge(laser_abs, bom, how='left')
    laser_finally_absence = laser_absence.drop_duplicates(subset='母件料號', keep='first', inplace=False)
    laser_absence_2 = laser_finally_absence[laser_finally_absence['原料成本'].isnull()]['母件料號']

    bending_absence = pd.merge(bending_abs, bom, how='left')
    bending_finally_absence = bending_absence.drop_duplicates(subset='母件料號', keep='first', inplace=False)
    bending_absence_2 = bending_finally_absence[bending_finally_absence['原料成本'].isnull()]['母件料號']

    # welding_absence = pd.merge(welding_abs, bom, how='left')
    # welding_finally_absence = welding_absence.drop_duplicates(subset='母件料號', keep='first', inplace=False)
    # welding_laser_absence_2 = welding_finally_absence[welding_finally_absence['原料成本'].isnull()]['母件料號']

    concatenated = pd.concat([laser_absence_2, bending_absence_2])
    concatenated = concatenated.drop_duplicates(keep='first', inplace=False)
    return concatenated


# 把各個DATA根據BOM合併起來
# 參數:(BOM,攤完成本後且乘上子件數量的雷射DATA,攤完成本後且乘上子件數量的折床DATA,攤完成本後且乘上子件數量的焊接DATA,,)
def merge_these_table_with_bom(bom, laser_, bending_, welding_=None, typesetting=None, lacquer=None, combination=None,
                               amount=None):

    def count_the_cost(x):  # 這邊是處理一些找不到的母件用的規則
        if x['母件料號'] == '0032005003G':
            return '003-2005-003-P'
        elif x['母件料號'] == '0037010016A':
            return '0037010016'
        elif x['母件料號'] == '0037010018B':
            return '003-7010-018'
        elif x['母件料號'] == '2090001035C':
            return '2090001035A'
        elif x['母件料號'] == '8031050002':
            return '803-1050-002'
        else:
            pass

    # 目前合併時候不再需要排版了
    # 這邊是處理一些額外的母件規則，能夠讓一些找不到母件編號的東西能夠對應的到
    # typesetting['母件料號2'] = typesetting.apply(lambda x: count_the_cost(x), axis=1)
    # grouped = typesetting.groupby('母件料號2')  # 雷射成本
    # agg = grouped['子件排版單一成本'].agg(np.sum)
    # typesetting_parent2_cost = pd.DataFrame(agg)
    # typesetting_parent2_cost.reset_index(inplace=True)
    # typesetting_parent2_cost.columns = ['母件料號', '子件排版單一成本2']

    # 雷射成本FOR找不到的母件
    laser_['母件料號2'] = laser_.apply(lambda x: count_the_cost(x), axis=1)
    grouped = laser_.groupby('母件料號2')
    agg = grouped['子件雷射單一成本'].agg(np.sum)
    laser_parent2_cost = pd.DataFrame(agg)
    laser_parent2_cost.reset_index(inplace=True)
    laser_parent2_cost.columns = ['母件料號', '子件雷射單一成本2']

    # 折床成本FOR找不到的母件
    bending_['母件料號2'] = bending_.apply(lambda x: count_the_cost(x), axis=1)
    grouped = bending_.groupby('母件料號2')  #
    agg = grouped['子件折床單一成本'].agg(np.sum)
    bending_parent2_cost = pd.DataFrame(agg)
    bending_parent2_cost.reset_index(inplace=True)
    bending_parent2_cost.columns = ['母件料號', '子件折床單一成本2']

    # 焊接成本FOR找不到的母件
    # welding_['母件料號2'] = welding_.apply(lambda x: count_the_cost(x), axis=1)
    # grouped = welding_.groupby('母件料號2')
    # agg = grouped['母件焊接單一成本'].agg(np.sum)
    # welding_parent2_cost = pd.DataFrame(agg)
    # welding_parent2_cost.reset_index(inplace=True)
    # welding_parent2_cost.columns = ['母件料號', '母件焊接單一成本2']

    # 把前面算完的單一子件GROUPBY
    # grouped = typesetting.groupby('母件料號')  # 排版成本
    # agg = grouped['子件排版單一成本'].agg(np.sum)
    # typesetting_parent_cost = pd.DataFrame(agg)
    # typesetting_parent_cost.reset_index(inplace=True)

    # 雷射成本
    grouped = laser_.groupby('母件料號')
    agg = grouped['子件雷射單一成本'].agg(np.sum)
    laser_parent_cost = pd.DataFrame(agg)
    laser_parent_cost.reset_index(inplace=True)

    # 折床成本
    grouped = bending_.groupby('母件料號')
    agg = grouped['子件折床單一成本'].agg(np.sum)
    bending_parent_cost = pd.DataFrame(agg)
    bending_parent_cost.reset_index(inplace=True)

    # 焊接成本
    # grouped = welding_.groupby('母件料號')
    # agg = grouped['母件焊接單一成本'].agg(np.sum)
    # welding_parent_cost = pd.DataFrame(agg)
    # welding_parent_cost.reset_index(inplace=True)

    # BOM+ 雷射+雷射2+ 折床+折床2+ 焊接+焊接2
    bom_df_with_ts = pd.merge(pd.merge(bom, laser_parent_cost, how='left'), laser_parent2_cost, how='left')
    bom_df_with_ts_ls = pd.merge(pd.merge(bom_df_with_ts, bending_parent_cost, how='left'), bending_parent2_cost,
                                 how='left')
    # bom_df_with_ts_ls_bd = pd.merge(pd.merge(bom_df_with_ts_ls, welding_parent_cost, how='left'), welding_parent2_cost
    #                                 ,how='left')
    return bom_df_with_ts_ls

    # 合起來
    # bom_df_with_ts = pd.merge( pd.merge(bom, typesetting_parent_cost, how='left'), typesetting_parent2_cost, how='left')
    # bom_df_with_ts_ls = pd.merge( pd.merge(bom_df_with_ts,laser_parent_cost, how='left'), laser_parent2_cost, how='left')
    # bom_df_with_ts_ls_bd = pd.merge(pd.merge(bom_df_with_ts_ls,bending_parent_cost, how='left'),bending_parent2_cost, how='left')
    # bom_df_with_ts_ls_bd_wd = pd.merge(pd.merge(bom_df_with_ts_ls_bd, welding_parent_cost, how='left'), welding_parent2_cost, how='left')
    # bom_df_with_ts_ls_bd_wd_lq= pd.merge(bom_df_with_ts_ls_bd_wd, lacquer, how='left')
    # bom_df_with_ts_ls_bd_wd_lq_cb = pd.merge(bom_df_with_ts_ls_bd_wd_lq, combination, how='left')
    # return bom_df_with_ts_ls_bd_wd_lq_cb
# ====================================================以上為合拼各表的CODE====================================================


def get_lacquer_and_combination_table(combination_or_lacquer):
    combination_cost = pd.read_excel('-臨時組立母件單一成本.xlsx', sheet_name=0)  # 會計整理過的五金件成本
    combination_cost = combination_cost[['料號', '標準加工成本']]  # 只拿我要的欄位
    combination_cost.columns = ['母件料號', '母件組立單一成本']  # 改名等下MERGE用

    lacquer_cost = pd.read_excel('-臨時噴漆母件單一成本.xlsx', sheet_name=1)  # 會計整理過的五金件成本
    lacquer_cost = lacquer_cost[['列標籤', '單一噴漆人工']]  # 只拿我要的欄位
    lacquer_cost.columns = ['母件料號', '母件噴漆單一成本']  # 改名等下MERGE用

    if combination_or_lacquer == 'combination':
        return combination_cost

    if combination_or_lacquer == 'lacquer':
        return lacquer_cost


# ====================================================以上為暫時讀取噴漆、組立報表的CODE====================================================


def outsourcing(bom_with_bunch_of_columns):  # 把前面跟BOM結合在一起的一大堆欄位丟進來
    outsourcing_cost = pd.read_excel('-0908最新會計統整後的原料成本.xlsx', sheet_name=5)  # 會計整理過的五金件成本
    outsourcing_cost.columns = [outsourcing_cost.iloc[0]]
    outsourcing_cost = outsourcing_cost.iloc[2:, 0:]
    outsourcing_cost.columns = ['母件料號', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '雷射倉', '折型倉', '焊接倉',
                                '烤漆倉', '加工其他倉', '染黑倉', '沖孔倉']  # 只拿我要的欄位
    outsourcing_cost = outsourcing_cost[['母件料號', '雷射倉', '折型倉', '焊接倉', '烤漆倉', '加工其他倉', '染黑倉', '沖孔倉']]  # 只拿我要的欄位
    outsourcing_cost.columns = ['母件料號', '雷射委外', '折床委外', '焊接委外', '噴漆委外', '加工其他倉', '染黑委外', '沖孔委外']  # 改名等下MERGE用
    bom_with_outsourcing_cost = pd.merge(bom_with_bunch_of_columns, outsourcing_cost, how='left')

    bom_with_outsourcing_cost = bom_with_outsourcing_cost[['母件料號', '母件類別',
                                                           '原料成本',
                                                           '子件排版單一成本', '子件排版單一成本2',
                                                           '子件雷射單一成本', '子件雷射單一成本2', '雷射委外',
                                                           '子件折床單一成本', '子件折床單一成本2', '折床委外',
                                                           '母件焊接單一成本', '母件焊接單一成本2', '焊接委外',
                                                           '母件噴漆單一成本', '噴漆委外',
                                                           '母件組立單一成本',
                                                           '加工其他倉', '染黑委外', '沖孔委外']]  # rearrange these columns sequence

    bom_with_outsourcing_cost.columns = ['母件料號', '母件類別',
                                         '原料成本',
                                         '排版成本', '排版成本2',
                                         '雷射成本', '雷射成本2', '雷射委外',
                                         '折床成本', '折床成本2', '折床委外',
                                         '焊接成本', '焊接成本2', '焊接委外',
                                         '噴漆成本', '噴漆委外',
                                         '組立成本',
                                         '加工其他倉', '染黑委外', '沖孔委外']

    bom_with_outsourcing_cost.drop_duplicates(subset='母件料號', keep='first', inplace=True)

    return bom_with_outsourcing_cost


# ====================================================以上為暫時讀取委外DATA的CODE====================================================


if __name__ == '__main__':
    start = time.time()

    # 拿DATA
    laser = connect_to_mysql('laser', 'svfs', '2020-05-31 00:00:00', '2020-06-27 23:59:59')  # 取得雷射data  要選開始跟結束日期
    typesetting_laser = connect_to_mysql('laser', 'qrcode_label')  # 取得排版圖data
    bom_laser = connect_to_mysql('laser', 'smbsource')  # 取得bom表data
    # 處理DATA及異常值
    laser_data = pre_processing('laser', laser)  # 把雷射data丟進去做前處理，做完前處理後的雷射DATA
    laser_data_with_name = merge_from_typesetting_and_bom(laser_data, typesetting_laser, bom_laser)  # 把子件、數量、母件通通拉了進來
    original_laser_data = deal_with_abnormal('laser', laser_data_with_name)  # 處理異常數據:子件數量變1, 用子件去幫母件改名

    # **********原生DATA這邊就可以直接匯出了**********
    #original_laser_data.to_csv('original_laser_data0531-0627.csv', index=False, header=True, encoding='utf_8_sig')

    # 開始加上各種額外資訊，去算它的耗時及成本
    # 1.這個是統計它的排版圖的子件總數量,且跟前面data merge起來
    laser_data_with_amount = amount_of_typesetting(typesetting_laser, original_laser_data)
    # 2.加上薪資表
    laser_data_with_amount_wage = get_wage_table('laser', laser_data_with_amount)
    almost_completed_laser_data = divided_consumption_cost('laser', laser_data_with_amount_wage)

    # 這邊做個拿雷射的DATA去做排版的薪資 暫時用而已 基本上就是雷射計算流程，薪資換排版而已
    #   typesetting_data_with_amount_wage = get_wage_table('typesetting',laser_data_with_amount)   # 這個之後把它刪掉
    #   almost_completed_typesetting_data = divided_consumption_cost('laser', typesetting_data_with_amount_wage)  # 這個之後把它刪掉

    # 3.最後一步算出單一雷射子件成本(single/multiple=有沒有算上需求數量)
    real_laser_cost = cost_per_one('laser', almost_completed_laser_data, bom_laser,'single')
    #   real_typesetting_cost = cost_per_one('typesetting', almost_completed_typesetting_data, bom_laser,'multiple')  # 這個之後把它刪掉

    # **********匯出雷射單一子件成本**********
    # export_to_excel(real_laser_cost, '1雷射_0913-0919')
    # export_to_excel(real_typesetting_cost, '0排版_0906-0912')    # 這個之後把它刪掉

    # ===================================================以上為雷射成本的部份===================================================

    # 拿DATA
    bending = connect_to_mysql('bending', 'svfs', '2020-05-31 00:00:00', '2020-06-27 23:59:59')  # 取得折床data  要選開始跟結束日期
    bom_bending = connect_to_mysql('bending', 'smbsource')  # 取得BOM表
    # 處理DATA及異常值
    bending_data = pre_processing('bending', bending)  # 把折床data丟進去做前處理，做完前處理後的折床DATA
    bending_data_with_name = merge_from_bom(bending_data, bom_bending)
    original_bending_data = deal_with_abnormal('bending', bending_data_with_name)

    # **********原生DATA這邊就可以直接匯出了**********
    # original_bending_data.to_csv('original_bending_data0831-0904.csv', index=False, header=True, encoding='utf_8_sig')

    # 開始加上各種額外資訊，去算它的耗時及成本
    # 1.加上薪資表
    bending_data_with_amount_wage = get_wage_table('bending', original_bending_data)
    almost_completed_bending_data = divided_consumption_cost('bending', bending_data_with_amount_wage)
    # 2.最後一步算出單一折床子件成本 (single/multiple=有沒有算上需求數量)
    real_bending_cost = cost_per_one('bending', almost_completed_bending_data, bom_bending,'single')

    # **********匯出折床單一子件成本**********
    # export_to_excel(real_bending_cost, '2折床_0913-0919')

    # ===================================================以上為折床成本的部份===================================================

    # 拿DATA
    welding = connect_to_mysql('welding','welding_report','2020-05-31 00:00:00','2020-06-27 23:59:59') # 取得焊接data  要選開始跟結束日期  #9月10跟會計用成只抓結束時間
    worker_name = connect_to_mysql('welding', 'welding_report2') # 這邊是拿工人的名字
    # 處理DATA及異常值
    welding_data = pre_processing('welding', welding)  # 把雷射data丟進去做前處理，做完前處理後的雷射DATA
    welding_data_with_name = merge_from_worker_name(welding_data, worker_name) #加上工人名字的焊接DATA
    welding_data_with_name_time = merge_from_welding_standard_time(welding_data_with_name)  # 加上標準工時去算的DATA
    original_welding_data = extra_processing_for_welding_data(welding_data_with_name_time)  # 幫剛剛加進來的標準工時去算週總耗時

    # **********原生DATA這邊就可以直接匯出了**********
    # original_welding_data.to_excel('original_welding_data0830-0905.xlsx', index=False, header=True, encoding='utf_8_sig')

    # 開始加上各種額外資訊，去算它的耗時及成本
    # 1.加上薪資表
    welding_data_with_name_wage = get_wage_table('welding', original_welding_data)
    almost_completed_welding_data = divided_consumption_cost('welding', welding_data_with_name_wage)#,'week') #這個'week'如果要加時就代表他是處理周的東西
    # 2.最後一步算出單一折床子件成本
    real_welding_cost = cost_per_one('welding', almost_completed_welding_data)
    # **********匯出折床單一子件成本**********
    # export_to_excel(real_welding_cost, '3焊接_0906-0912')

    # ===================================================以上為焊接成本的部份===================================================

    material_cost = get_material_data_and_bom()  # (BOM+料錢) 這個是拿會計給我的BOM，已經暫時把料錢算好了

    # 這邊臨時把噴漆跟組立的成本拉進來拼在一起
    # real_lacquer_cost = get_lacquer_and_combination_table('lacquer') # 基本上這邊之後要砍掉，要幫它們寫DEF，現在只是暫時這樣撈資料而已
    # real_combination_cost = get_lacquer_and_combination_table('combination') # 基本上這邊之後要重砍，要幫它們寫DEF，現在只是暫時這樣撈資料而已

    # 偵察缺的BOM
    # absence_parent = parent_absence_detection(material_cost, real_laser_cost, real_bending_cost)
    # export_to_excel(absence_parent, '0913-0919缺的BOM')

    # 這邊是把BOM+料錢+排版子件成本+雷射子件成本+折床子件成本+焊接子件成本
    multiple_real_laser_cost = cost_per_one('laser', almost_completed_laser_data, bom_laser, 'multiple')  # 雷射算上需求數量 
    multiple_real_bending_cost = cost_per_one('bending', almost_completed_bending_data, bom_bending, 'multiple')  # 折床算上需求數量
    merged_cost_table = merge_these_table_with_bom(material_cost, multiple_real_laser_cost, multiple_real_bending_cost)
    # merged_cost_table2 = outsourcing(merged_cost_table)

    export_to_excel(merged_cost_table, '0913-0919初版成本表')

    # =============================================以上為料錢及合併的計算的部份=============================================

    # 把前面生成好的雷、折、焊DATA拿來concat，因為電費的分攤邏輯是1廠:雷折焊一起的加總週耗時，2廠:噴漆的加總週耗時，並加上週總耗時(分鐘)
    data_miss_electricity_cost = concat_data_with_electricity(almost_completed_laser_data,
                                                              almost_completed_bending_data,
                                                              almost_completed_welding_data)

    # 把前面攤好的data丟進來，直接對應data裡面的week跟工廠，並算上分攤電費
    almost_completed_electricity_data = get_table_and_calculation(data_miss_electricity_cost)

    # 把上面concat完的data，分開來，得出各自的單一子件/母件電費
    print(electricity_cost_per_one('laser', almost_completed_electricity_data, single_or_multiple='single'))
    print(electricity_cost_per_one('bending', almost_completed_electricity_data, single_or_multiple='single'))
    print(electricity_cost_per_one('welding', almost_completed_electricity_data))

    # =================================================以上為電費計算的部份=================================================
    end = time.time()
    print('耗時:',end - start)

    # 薪資記得要一直補上

    # 從古到今的排雷折焊單一子件成本要調整的東西: 生產日期、
    #                              檔名日期、
    #                              焊接的話almost_completed_welding_data後面的參數week*要去掉，不然數值會變超小
    #                              merge_these_table_with_bom那邊的排版要打開

    # 每週的雷折焊單一子件成本: 生產日期、
    #                 檔名日期、
    #                 焊接的話almost_completed_welding_data後面的參數week*要補上

    # 每週的合併報表: 生產日期、
    #              檔名日期
    #              焊接的話almost_completed_welding_data後面的參數week*要補上
