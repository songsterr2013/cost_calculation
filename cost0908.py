import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import timedelta
import numpy as np
import time
# 連線資料庫
def connect_to_mysql(task,database,start=None,end=None): # 參數:(報表類型, svfs=雷射/qrcode=排版圖/smbsource=bom表,開始時間跟結束時間*可選)

    try:
        cnx = mysql.connector.connect(host='59.120.87.160',
                                      user='smb',
                                      passwd='Csie3621',
                                     )
        if cnx.is_connected():
            # check the version of the db
            db_Info = cnx.get_server_info()
            cursor = cnx.cursor(named_tuple=True)
            print('db_version:', db_Info)

    except Error as e:
        print("資料庫連接失敗：", e)

    if task == 'laser':

        if database=='svfs':
            cursor.execute("SELECT machine, program_name, start_datetime, end_datetime, TIMEDIFF(end_datetime,start_datetime) as `delta_time`, results_count, bad_count \
                            FROM svfs.sheets_log \
                            WHERE start_datetime >= '{}' and end_datetime <='{}' ORDER BY start_datetime ASC".format(start,end))
            sheets_log = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the laser data !')
            return sheets_log

        elif database=='qrcode_label':
            cursor.execute("SELECT program_id, metal_no, amount \
                            FROM qrcode_label.metal_no_list ")
            data_qrcode = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the metal_no_list data !')
            return data_qrcode

        elif database=='smbsource':
            cursor.execute("SELECT product_id,parent_id,amount \
                            FROM smbsource.bom \
                            WHERE mp_id = 'MB' ")
            parent = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the bom data !')
            return parent

    elif task == 'bending':

        if database=='svfs':
            cursor.execute("SELECT machine , part_name , result_start_time , result_end_time , TIMEDIFF(result_end_time,result_start_time) as `delta_time` , result_count , result_badcount \
                            FROM svfs.parts_log\
                            WHERE result_start_time >= '{}' and result_end_time <='{}' ORDER BY result_start_time ASC".format(start,end))
            parts_log = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the bending data !')
            return parts_log

        elif database=='smbsource':
            cursor.execute("SELECT product_id,parent_id,amount \
                            FROM smbsource.bom\
                            WHERE mp_id = 'MC' ")
            parent = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the bom data !')
            return parent

    elif task == 'welding':

        if database=='welding_report':
            cursor.execute("SELECT worker_id, parent_id, result_start_time, result_end_time, TIMEDIFF(result_end_time,result_start_time) as `delta_time`,expect_amount, result_amount\
                            FROM welding_report.welding_log\
                            WHERE result_start_time >= '{}' and result_end_time <='{}' ORDER BY result_start_time ASC".format(start,end))
            welding_log = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the welding data !')
            return welding_log

        elif database=='welding_report2': #這邊只好叫它2了，因為它剛好跟上面撞同一個DATABASE
            cursor.execute("SELECT username , realname \
                            FROM welding_report.weldworker")
            data_name = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the worker name data !')
            return data_name

    elif task =='bom':

        if database=='bom':
            cursor.execute("SELECT *\
                            FROM smbsource.bom")
            bom_data = cursor.fetchall()
            cursor.close()
            cnx.close()
            print('successfully got the bom data !')
            return bom_data

def pre_processing(task,data): # 參數:(task=雷射/折床/焊接的報表處理, data=前面資料庫拿出來的DATA)   **********這邊只是雷射的前處理，其他DATA或許不是這樣子

    if task=='laser':
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
                print('i have already reset and the new week_number is:',week_number)
                count_time += index[4]  # 裡面的所有耗時加總
                index_list.append(index)  # 把屬於下一週的東西新增到List

            if number == len(data) - 1:  # 當它來到最後一行時
                print('Here is the last line')
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

    elif task=='bending' or 'welding':  #折床跟焊接進同一個

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
                print('i have already reset and the new week_number is:', week_number)
                count_time += index[4]  # 裡面的所有耗時加總
                index_list.append(index)  # 把屬於下一週的東西新增到List

            if number == len(data) - 1:  # 當它來到最後一行時
                print('Here is the last line')
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


def merge_from_typesetting_and_bom(sheets_log,data_qrcode,parent):# 參數:(前處理後的雷射DATA,排版圖DATA,BOM表)  前處理後的雷射DATA加上子件料號跟數量,母件名稱

    #缺子件名稱的雷射報表
    df_sheets_log = pd.DataFrame(sheets_log)
    df_sheets_log.columns=['機台','排版圖編號','週數','週總耗時', '起始時間', '結束時間', '耗時','完成數量', '失敗數量']

    #從排版圖去撈母件名稱
    df_components = pd.DataFrame(data_qrcode)
    df_components.columns=['排版圖編號','子件料號','子件數量']

    #merge完子件料號跟數量
    data = pd.merge(df_sheets_log, df_components, how='left')

    #從bom表去拿母件料號
    df_parents = pd.DataFrame(parent)
    df_parents.columns = ['子件料號', '母件料號','需求數量']

    data_merged = pd.merge(data, df_parents, how='left')

    return data_merged


def merge_from_bom(bending_data,parent): # 參數:(前處理後的折床DATA,BOM表)  前處理後的折床DATA加上子件料號跟數量,母件名稱

    # 缺母件名稱的折床報表
    df_parts_log = pd.DataFrame(bending_data)
    df_parts_log.columns = ['機台', '子件料號', '週數', '週總耗時', '起始時間', '結束時間', '耗時', '完成數量', '失敗數量']

    # 從bom去撈母件名稱
    df_parent = pd.DataFrame(parent)
    df_parent.columns = ['子件料號', '母件料號', '需求數量']

    data_merged = pd.merge(df_parts_log, df_parent, how='left')
    # 直接合起來最快，有些顯示NAN，是因為這個子件根本就沒有折床工序卻莫名奇妙地出現了，
    # 有的是新子件

    return data_merged


def merge_from_worker_name(welding_data,name): # 參數:(前處理後的焊接DATA,工人名稱)  前處理後的焊接DATA加上子件料號跟數量,母件名稱

    #缺工人名稱的焊接報表
    df_parents_log = pd.DataFrame(welding_data)
    df_parents_log.columns=['工人編號','母件料號','週數','週總耗時', '起始時間', '結束時間', '耗時','預期完成數量', '實際生產數量']

    #從weld_worker去撈工人名稱
    df_name = pd.DataFrame(name)
    df_name.columns=['工人編號','工人']

    data_merged = pd.merge(df_parents_log, df_name, how='left')
    # 直接合起來最快

    return data_merged

def merge_from_welding_standard_time(data): # 參數:(要跟標準工時接在一起的焊接DATA)  這邊的return比較特別，等之後焊接data都正常時才能改掉

    # 讀取標準工時表
    standard_time_table = pd.read_csv('焊接標準工時.csv')
    standard_time_table = standard_time_table[['product_no', 'time']]  # 只拿我要的COLUMNS
    standard_time_table.columns = ['母件料號', '標準工時']

    data_merged_2 = pd.merge(data, standard_time_table, how='left')

    return data_merged_2[data_merged_2['實際生產數量']>=1]   #有些實際生產是0的是工人操作不當或我們收data的問題，它或許是一個真實的數據，但錯的太誇張了，一個母件焊22天???

def extra_processing_for_welding_data(data):

    data_transition = np.array(data)
    data_transition = data_transition.tolist()

    data_list = []

    for i in data_transition:
        if type(i[10]) == str:
            data_list.append([i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], int(i[10])])
        else:
            data_list.append([i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], 0])  # 把NAN值直接變0 ，不然後面根本運算不了

    week_number = 1  # 從哪一周開始算
    count_time = 0  # 耗時加總用
    index_list = []  # 把這一週的東西新增進去

    real_data = []
    for number, index in enumerate(data_list):

        if week_number == index[2]:  # 第一次進的會是23
            count_time += index[10]  # 裡面的所有耗時加總
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
                                  count_time
                                  ])  # list裡面的東西

            count_time = 0  # 重畳
            index_list = []  # 重畳
            week_number = index[2]  # 因為跳到下一週了，它本身會進入elif，這邊的週數也變成了那一週

            count_time += index[10]  # 裡面的所有耗時加總
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
                                  count_time
                                  ])  # list裡面的東西

    the_real_data = pd.DataFrame(real_data)
    the_real_data.columns = ['工人編號', '母件料號', '週數', '週總耗時', '起始時間', '結束時間', '耗時', '預期完成數量', '實際生產數量', '工人', '標準工時','週總標準工時耗時']

    return the_real_data

def deal_with_abnormal(task,real_data):  # 參數:() 處理子件數量為0及有母件料號的異常值()

    if task=='laser':

        for index, row in real_data.iterrows():

            if row[10] >= 1:  # 處理子件數量為0的時候，把它通通變1，他的邏輯就會變成是他有分攤到耗時
                pass

            else:
                real_data.iloc[index, 10] = 1

            if type(row[11]) != str:  # 處理沒有母件料號的子件，用-分開，然後取第0個或=子件料號

                if type(row[9]) == str:

                    if len(row[9].split('-')) >= 2:  # 不是為1的話他只有小部份例外，他比較長的字串在中間，不要管他，反正都取第0個
                        parent = [index, row[9].split('-')[0]]
                        real_data.iloc[parent[0], 11] = parent[1] # '*' +

                    else:  # 就都取自己就對了
                        parent = [index, row[9]]
                        real_data.iloc[parent[0], 11] = parent[1] # '*' +
        return real_data

    elif task=='bending':

        for index, row in real_data.iterrows():

            if type(row[9]) == float:

                if len(row[1].split('-')) >= 2:
                    parent = [index, row[1].split('-')[0]]
                    real_data.iloc[parent[0], 9] = parent[1] # '*' +

                else:
                    parent = [index, row[1]]
                    real_data.iloc[parent[0], 9] = parent[1] # '*' +

            if row[7] == 0:  # 這個地方處理的是'如果完成數量為0',前面用的「數量」就代表它的完成數量，如果也是0的話就只能是1了

                if row[10] >= 1:
                    real_data.iloc[index, 7] = row[10]

                else:
                    real_data.iloc[index, 7] = 1

        return real_data


def amount_of_typesetting(data_qrcode,real_data): # 參數:(排版圖中的子件總數,原生雷射DATA) 把排版圖中的子件總數併到雷射DATA，並處理異常值
    df_components = pd.DataFrame(data_qrcode)
    df_components.columns=['排版圖編號','子件料號','子件數量']

    agg=df_components.groupby('排版圖編號')['子件數量'].agg(np.sum)
    amounts=pd.DataFrame(agg)
    amounts.reset_index(inplace=True)
    amounts.columns=['排版圖編號','排版圖子件數']

    real_data_no2 = pd.merge(real_data, amounts, how='left')

    for index, row in real_data_no2.iterrows():

        if row[13] >= 1:  # 處理排版圖子件數為0的時候
            pass
        else:
            real_data_no2.iloc[index, 13] = 1

    return real_data_no2


def get_wage_table(task,data): # 參數:(處理完後的雷射DATA)  加上薪資表欄位，準備算

    wage_table = pd.read_excel('-歷史薪水.xlsx')  # 讀取薪資表

    if task == 'typesetting':

        agg = wage_table.groupby('週數')['排版薪資'].agg(np.sum)
        real_wage_table = pd.DataFrame(agg)
        real_wage_table.reset_index(inplace=True)

        real_data_no3 = pd.merge(data, real_wage_table, how='left')

        return real_data_no3

    if task == 'laser':

        agg = wage_table.groupby('週數')['雷射薪資'].agg(np.sum)
        real_wage_table = pd.DataFrame(agg)
        real_wage_table.reset_index(inplace=True)

        real_data_no3 = pd.merge(data, real_wage_table, how='left')

        return real_data_no3

    elif task == 'bending':

        agg = wage_table.groupby('週數')['折床薪資'].agg(np.sum)
        real_wage_table = pd.DataFrame(agg)
        real_wage_table.reset_index(inplace=True)

        real_data_no3 = pd.merge(data, real_wage_table, how='left')

        return real_data_no3

    elif task == 'welding':

        agg = wage_table.groupby('週數')['焊接薪資'].agg(np.sum)
        real_wage_table = pd.DataFrame(agg)
        real_wage_table.reset_index(inplace=True)

        real_data_no3 = pd.merge(data, real_wage_table, how='left')

        return real_data_no3


def divied_comsumption_cost(task,data): # 參數:(處理完後的雷射DATA)  算分攤耗時跟分攤成本

    if task == 'laser':

        divided_time=[]
        for index ,row in data.iterrows():
            divided_time.append(row[6]*row[10]/row[13])
        data['分攤耗時']=divided_time

        divided_cost=[]
        for index ,row in data.iterrows():
            divided_cost.append(row[14]*row[15]/row[3])
        data['分攤成本']=divided_cost

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

        divided_cost = []
        for index, row in data.iterrows():
            divided_cost.append(row[12] * row[10] / row[11])
        data['總成本'] = divided_cost

        return data


def cost_per_one(task,data,amount=None): # 參數:(處理完後的雷射DATA)  最後算出單一雷射子件成本

    if task == 'typesetting':

        true_typesetting_cost = []
        grouped = data.groupby(['子件料號', '母件料號'])  # 用 groupby的方法處理
        agg = grouped['子件數量', '分攤成本'].agg(np.sum)
        typesetting_cost = pd.DataFrame(agg)
        typesetting_cost.reset_index(inplace=True)

        for i, q in zip(typesetting_cost['分攤成本'], typesetting_cost['子件數量']):
            true_typesetting_cost.append(i / q)

        typesetting_cost['子件排版單一成本'] = true_typesetting_cost
        df_parents = pd.DataFrame(amount)
        df_parents.columns = ['子件料號', '母件料號', '需求數量']
        typesetting_cost_2 = pd.merge(typesetting_cost, df_parents, how='left')

        def sum_up_the_component(x):
            if x['需求數量'] >= 1:
                return x['子件排版單一成本'] * x['需求數量']
            else:
                return x['子件排版單一成本']

        typesetting_cost_2['子件排版單一成本'] = typesetting_cost_2.apply(lambda x: sum_up_the_component(x), axis=1)
        return typesetting_cost_2



    if task == 'laser':

        true_laser_cost = []
        grouped = data.groupby(['子件料號', '母件料號'])  # 用 groupby的方法處理
        agg = grouped['子件數量', '分攤成本'].agg(np.sum)
        laser_cost = pd.DataFrame(agg)
        laser_cost.reset_index(inplace=True)

        for i, q in zip(laser_cost['分攤成本'], laser_cost['子件數量']):
            true_laser_cost.append(i / q)

        laser_cost['子件雷射單一成本'] = true_laser_cost
        df_parents = pd.DataFrame(amount)
        df_parents.columns = ['子件料號', '母件料號', '需求數量']
        laser_cost_2 = pd.merge(laser_cost, df_parents, how='left')

        def sum_up_the_component(x):
            if x['需求數量'] >= 1:
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
        df_parents = pd.DataFrame(amount)
        df_parents.columns = ['子件料號', '母件料號', '需求數量']
        bending_cost_2 = pd.merge(bending_cost, df_parents, how='left')

        def sum_up_the_component(x):
            if x['需求數量'] >= 1:
                return x['子件折床單一成本'] * x['需求數量']
            else:
                return x['子件折床單一成本']

        bending_cost_2['子件折床單一成本'] = bending_cost_2.apply(lambda x: sum_up_the_component(x), axis=1)
        return bending_cost_2

    if task == 'welding':

        true_welding_cost = []
        grouped = data.groupby('母件料號')  # 用 groupby的方法處理
        agg = grouped['實際生產數量', '總成本'].agg(np.sum)
        welding_cost = pd.DataFrame(agg)
        welding_cost.reset_index(inplace=True)

        for i, q in zip(welding_cost['總成本'], welding_cost['實際生產數量']):
            true_welding_cost.append(i / q)

        welding_cost['母件焊接單一成本'] = true_welding_cost
        return welding_cost


def abnormal_parents(data): # 參數:(前面產出來的雷射DATA)     雷射生產了一些出貨表不用出的東西，經過這個def去比對，找出異常data
    abnormal = pd.read_excel('檢查母件異常，該要有但雷射沒有的.xls')  # 讀取出貨表
    abnormal.rename(columns={'產品編號': '母件料號'}, inplace=True)
    abnormal.drop_duplicates(subset='母件料號', keep='first', inplace=True)
    abnormal_data = pd.merge(data, abnormal, how='left')
    abnormal_data_list=abnormal_data[abnormal_data['單據號碼'].isnull()]
    abnormal_data_list.to_excel('雷射生產異常data.xlsx',index=False, header=True, encoding='utf_8_sig')


def export_to_excel(data,rename): # 參數:(DATA,命名) 最後匯出成CSV檔的話都是來這一步
    data.to_excel('{}.xlsx'.format(rename), index=False, header=True, encoding='utf_8_sig')

# ====================================================以上大多為工錢的CODE====================================================



def get_material_data_and_bom(): #拉會計給我的BOM，跟從進貨表中整理好的進貨成本，去算出料錢，之後怎麼樣一直收DATA就要大改這邊了
    material_hardware = pd.read_excel('-最新會計統整後的原料成本.xlsx', sheet_name=1)  # 會計整理過的五金件成本
    material_hardware = material_hardware[['零件名稱', '進價']] # 只拿我要的欄位
    material_hardware.columns = ['零件名稱', '五金進價'] # 改名等下MERGE用

    material_iron = pd.read_excel('-最新會計統整後的原料成本.xlsx', sheet_name=3)  # 會計整理過的鈑材類成本
    material_iron = material_iron[['類別', '平均單價']] # 只拿我要的欄位
    material_iron.columns = ['產品類別', '鈑材平均單價'] # 改名等下MERGE用

    bom = pd.read_excel('-最新BOM表.xlsx', sheet_name=0) # 會計整理過後的BOM表，真實的BOM有60000多筆...要怎麼拿?
    bom_df = bom[['母件編號','母件名稱','母件類別','項次編號','零件名稱','版本','規格','數量','重量','面積','產品類別','排版','雷射','折床','焊接','總重','總面積']] # 只拿我要的欄位
    print('Successfully got these data!')

    bom_df_with_cost = pd.merge(bom_df, material_hardware, how='left') # BOM MERGE 五金件成本


    bom_df_with_cost2 = pd.merge(bom_df_with_cost, material_iron, how='left') # BOM MERGE 鈑材類成本
    print('Successfully merged!')

    bom_df_with_cost2 = bom_df_with_cost2.drop_duplicates(subset=None, keep='first', inplace=False) #去除重覆值，因為五金件有些名字重覆，導致會生出重覆DATA


    def count_the_cost(x):
        if x['產品類別'] in ['F01', 'F03', 'F05']:
            return x['五金進價'] * x['數量']
        else:
            return x['鈑材平均單價'] * x['總重']

    bom_df_with_cost2['原料成本'] = bom_df_with_cost2.apply(lambda x: count_the_cost(x), axis=1)

    grouped = bom_df_with_cost2.groupby(['母件編號', '母件類別'])  # 用groupby的方法處理
    agg = grouped['原料成本'].agg(np.sum)
    material_cost = pd.DataFrame(agg)
    material_cost.reset_index(inplace=True)
    material_cost.columns =['母件料號','母件類別','原料成本']

    return material_cost



# ====================================================以上為計算料錢的CODE====================================================



def get_electricity_table(which_factory,week): #參數:(哪一個電錶? , 哪一週的耗電?)

    electricity_bill = pd.read_excel('-歷史電費.xlsx')  # 讀取電費表
    electricity_bill = electricity_bill.iloc[1:, 0:]
    agg = electricity_bill.groupby('week')['comsuming_1', 'comsuming_2', 'comsuming_3', 'comsuming_4', 'comsuming_5', 'comsuming_6', 'comsuming_7'].agg(np.sum)
    electricity_bill_table = pd.DataFrame(agg)
    electricity_bill_table.reset_index(inplace=True)  #這邊就會整理好格式正確的電費表

    comsumption = [row[which_factory] for index, row in electricity_bill_table.iterrows() if row[0] == week + 1][0]
    print('How much does it cost:', comsumption)

    # 本廠1-後壁厝小段728-2地號(雙月計價)
    if which_factory == 1:
        magnification = 40  # only for this one
        rate = 5.05  # always get the highest one
        cost = (comsumption * magnification * rate) / 1.05  # without tax

    # 本廠2-光啟路92-2號(雙月計價)
    elif which_factory == 2:
        magnification = 40  # only for this one
        rate = 5.05  # always get the highest one
        cost = (comsumption * magnification * rate) / 1.05  # without tax

    # 本廠3-光啟段816地號 光啟路92-2號前 (單月計價)
    elif which_factory == 3:
        if week in [row[0] for index, row in electricity_bill_table.iterrows() if int(row[0]) >= 23 <= 40]:
            agreed_rate = (99 * 236.2) * 7 / 29  # summer rate
        else:
            agreed_rate = (99 * 173.2) * 7 / 29  # non-summer rate
        magnification = 80  # only for this one
        rate = 3.33  # always get the highest one
        cost = ((comsumption * magnification * rate) + (agreed_rate)) / 1.05  # without tax

    # 本廠4-光啟路92號後東(單月計價)
    elif which_factory == 4:
        if week in [row[0] for index, row in electricity_bill_table.iterrows() if int(row[0]) >= 23 <= 40]:
            agreed_rate = (99 * 236.2) * 7 / 34  # summer rate
        else:
            agreed_rate = (99 * 173.2) * 7 / 34  # non-summer rate
        magnification = 80  # only for this one
        rate = 2.58  # fixed rate
        cost = ((comsumption * magnification * rate) + (agreed_rate)) / 1.05  # without tax

    # 二廠1-後壁厝小段640-1地號 民權路37號前(雙月計價)
    elif which_factory == 5:
        magnification = 1  # only for this one
        rate = 5.05  # always get the highest one
        cost = (comsumption * magnification * rate) / 1.05  # without tax

    # 二廠2-民權路37號(單月計價)
    elif which_factory == 6:
        if week in [row[0] for index, row in electricity_bill_table.iterrows() if int(row[0]) >= 23 <= 40]:
            agreed_rate = (14 * 236.2) * 7 / 35  # summer rate
        else:
            agreed_rate = (14 * 173.2) * 7 / 35  # non-summer rate
        magnification = 1  # only for this one
        rate = 2.58  # fixed rate
        cost = ((comsumption * magnification * rate) + (agreed_rate)) / 1.05  # without tax

    # 二廠3-光啟路191-1號(單月計價)
    elif which_factory == 7:
        if week in [row[0] for index, row in electricity_bill_table.iterrows() if int(row[0]) >= 23 <= 40]:
            agreed_rate = (99 * 236.2) * 7 / 35  # summer rate
        else:
            agreed_rate = (99 * 173.2) * 7 / 35  # non-summer rate
        magnification = 80  # only for this one
        rate = 2.58  # fixed rate
        cost = ((comsumption * magnification * rate) + (agreed_rate)) / 1.05  # without tax

    return cost



# ====================================================以上為計算電費的CODE====================================================



def merge_these_table_with_bom(bom,typesetting,laser,bending,welding,lacquer,combination,amount=None):

    def count_the_cost(x):  #這邊是處理一些找不到的母件用的規則
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

    # 這邊是處理一些額外的母件規則，能夠讓一些找不到母件編號的東西能夠對應的到
    typesetting['母件料號2'] = typesetting.apply(lambda x: count_the_cost(x), axis=1)
    grouped = typesetting.groupby('母件料號2')  # 雷射成本
    agg = grouped['子件排版單一成本'].agg(np.sum)
    typesetting_parent2_cost = pd.DataFrame(agg)
    typesetting_parent2_cost.reset_index(inplace=True)
    typesetting_parent2_cost.columns = ['母件料號', '子件排版單一成本2']

    laser['母件料號2'] = laser.apply(lambda x: count_the_cost(x), axis=1)
    grouped = laser.groupby('母件料號2')  # 雷射成本
    agg = grouped['子件雷射單一成本'].agg(np.sum)
    laser_parent2_cost = pd.DataFrame(agg)
    laser_parent2_cost.reset_index(inplace=True)
    laser_parent2_cost.columns = ['母件料號', '子件雷射單一成本2']

    bending['母件料號2'] = bending.apply(lambda x: count_the_cost(x), axis=1)
    grouped = bending.groupby('母件料號2')  # 雷射成本
    agg = grouped['子件折床單一成本'].agg(np.sum)
    bending_parent2_cost = pd.DataFrame(agg)
    bending_parent2_cost.reset_index(inplace=True)
    bending_parent2_cost.columns = ['母件料號', '子件折床單一成本2']


    welding['母件料號2'] = welding.apply(lambda x: count_the_cost(x), axis=1)
    grouped = welding.groupby('母件料號2')  # 雷射成本
    agg = grouped['母件焊接單一成本'].agg(np.sum)
    welding_parent2_cost = pd.DataFrame(agg)
    welding_parent2_cost.reset_index(inplace=True)
    welding_parent2_cost.columns = ['母件料號', '母件焊接單一成本2']


    #把前面算完的單一子件GROUPBY
    grouped = typesetting.groupby('母件料號')  # 排版成本
    agg = grouped['子件排版單一成本'].agg(np.sum)
    typesetting_parent_cost = pd.DataFrame(agg)
    typesetting_parent_cost.reset_index(inplace=True)

    grouped = laser.groupby('母件料號')  # 雷射成本
    agg = grouped['子件雷射單一成本'].agg(np.sum)
    laser_parent_cost = pd.DataFrame(agg)
    laser_parent_cost.reset_index(inplace=True)

    grouped = bending.groupby('母件料號')  # 折床成本
    agg = grouped['子件折床單一成本'].agg(np.sum)
    bending_parent_cost = pd.DataFrame(agg)
    bending_parent_cost.reset_index(inplace=True)

    grouped = welding.groupby('母件料號')  # 焊接成本
    agg = grouped['母件焊接單一成本'].agg(np.sum)
    welding_parent_cost = pd.DataFrame(agg)
    welding_parent_cost.reset_index(inplace=True)

    #合起來
    bom_df_with_ts = pd.merge( pd.merge(bom, typesetting_parent_cost, how='left'), typesetting_parent2_cost, how='left')
    bom_df_with_ts_ls = pd.merge( pd.merge(bom_df_with_ts,laser_parent_cost, how='left'), laser_parent2_cost, how='left')
    bom_df_with_ts_ls_bd = pd.merge(pd.merge(bom_df_with_ts_ls,bending_parent_cost, how='left'),bending_parent2_cost, how='left')
    bom_df_with_ts_ls_bd_wd = pd.merge(pd.merge(bom_df_with_ts_ls_bd, welding_parent_cost, how='left'), welding_parent2_cost, how='left')
    bom_df_with_ts_ls_bd_wd_lq= pd.merge(bom_df_with_ts_ls_bd_wd, lacquer, how='left')
    bom_df_with_ts_ls_bd_wd_lq_cb = pd.merge(bom_df_with_ts_ls_bd_wd_lq, combination, how='left')

    return bom_df_with_ts_ls_bd_wd_lq_cb



# ====================================================以上為合拼各表的CODE====================================================


def get_lacquer_and_combination_table(combination_or_lacquer):
    combination_cost = pd.read_excel('-臨時組立母件單一成本.xlsx', sheet_name=0)  # 會計整理過的五金件成本
    combination_cost = combination_cost[['料號', '標準加工成本']] # 只拿我要的欄位
    combination_cost.columns = ['母件料號', '母件組立單一成本'] # 改名等下MERGE用

    lacquer_cost = pd.read_excel('-臨時噴漆母件單一成本.xlsx', sheet_name=1)  # 會計整理過的五金件成本
    lacquer_cost = lacquer_cost[['列標籤', '單一噴漆人工']] # 只拿我要的欄位
    lacquer_cost.columns = ['母件料號', '母件噴漆單一成本'] # 改名等下MERGE用

    if combination_or_lacquer == 'combination':
        return combination_cost

    if combination_or_lacquer == 'lacquer':
        return lacquer_cost

# ====================================================以上為暫時讀取噴漆、組立報表的CODE====================================================



def outsourcing(bom_with_bunch_of_columns): # 把前面跟BOM結合在一起的一大堆欄位丟進來
    outsourcing_cost = pd.read_excel('-委外用的TABLE.xlsx', sheet_name=6)  # 會計整理過的五金件成本
    outsourcing_cost.columns = outsourcing_cost.iloc[0]
    outsourcing_cost = outsourcing_cost.iloc[1:,0:]
    outsourcing_cost = outsourcing_cost[['列標籤', '雷射倉','折型倉','焊接倉','噴漆','加工其他倉','裁切-管']]  # 只拿我要的欄位
    outsourcing_cost.columns = ['母件料號', '雷射委外','折床委外','焊接委外','噴漆委外','加工其他倉','裁切-管']  # 改名等下MERGE用

    bom_with_outsourcing_cost = pd.merge(bom_with_bunch_of_columns, outsourcing_cost, how='left')

    bom_with_outsourcing_cost = bom_with_outsourcing_cost[['母件料號','母件類別',\
                                                           '原料成本',\
                                                           '子件排版單一成本','子件排版單一成本2',\
                                                           '子件雷射單一成本','子件雷射單一成本2','雷射委外',\
                                                           '子件折床單一成本','子件折床單一成本2','折床委外',\
                                                           '母件焊接單一成本','母件焊接單一成本2','焊接委外',\
                                                           '母件噴漆單一成本','噴漆委外',\
                                                           '母件組立單一成本',\
                                                           '加工其他倉','裁切-管']] #rearrange these columns sequence



    bom_with_outsourcing_cost.columns = ['母件料號','母件類別',\
                                        '原料成本',\
                                        '排版成本','排版成本2',\
                                        '雷射成本','雷射成本2','雷射委外',\
                                        '折床成本','折床成本2','折床委外',\
                                        '焊接成本','焊接成本2','焊接委外',\
                                        '噴漆成本','噴漆委外',\
                                        '組立成本',\
                                        '加工其他倉','裁切-管']

    bom_with_outsourcing_cost.drop_duplicates(subset='母件料號', keep='first', inplace=True)

    return bom_with_outsourcing_cost


# ====================================================以上為暫時讀取委外DATA的CODE====================================================



if __name__ == '__main__':
    start = time.time()
        # 拿DATA
    laser = connect_to_mysql('laser','svfs','2020-05-01 00:00:00','2020-08-30 23:59:59') # 取得雷射data  要選開始跟結束日期
    typesetting_laser = connect_to_mysql('laser','qrcode_label') # 取得排版圖data
    bom_laser = connect_to_mysql('laser','smbsource') # 取得bom表data

        # 處理DATA及異常值
    laser_data = pre_processing('laser', laser) # 把雷射data丟進去做前處理，做完前處理後的雷射DATA
    laser_data_with_name = merge_from_typesetting_and_bom(laser_data,typesetting_laser,bom_laser) # 把子件、數量、母件通通拉了進來
    original_laser_data = deal_with_abnormal('laser',laser_data_with_name) # 處理異常數據:子件數量變1, 用子件去幫母件改名

        # 原生DATA這邊就可以直接匯出了
    # original_laser_data.to_csv('original_laser_data0531-0627.csv', index=False, header=True, encoding='utf_8_sig')

        # 開始加上各種額外資訊，去算它的耗時及成本
        # 1.這個是統計它的排版圖的子件總數量,且跟前面data merge起來
    laser_data_with_amount = amount_of_typesetting(typesetting_laser,original_laser_data)
        
        # 2.加上薪資表
    laser_data_with_amount_wage = get_wage_table('laser',laser_data_with_amount)
    almost_completed_laser_data = divied_comsumption_cost('laser',laser_data_with_amount_wage)

    typesetting_data_with_amount_wage = get_wage_table('typesetting', laser_data_with_amount)   # 這邊做個拿雷射的DATA去做排版的薪資 暫時用而已 基本上就是雷射計算流程，薪資換排版而已
    almost_completed_typesetting_data = divied_comsumption_cost('laser', typesetting_data_with_amount_wage) # 這個之後把它刪掉
        
        # 3.最後一步算出單一雷射子件成本
    real_laser_cost = cost_per_one('laser',almost_completed_laser_data,bom_laser)
    real_typesetting_cost = cost_per_one('typesetting', almost_completed_typesetting_data,bom_laser) # 這個之後把它刪掉

        # 4.這邊是會計叫我做的查看雷射異常DATA(有需要再用)  記得要去deal_with_abnormal()把它的星號除掉，比較方便
    # abnormal_parents(real_laser_cost)
    
        # 匯出雷射單一子件成本
    #export_to_excel(real_laser_cost, '1雷射_最早-0830')
    #export_to_excel(real_typesetting_cost, '0排版_最早-0830')    # 這個之後把它刪掉
# ===================================================以上為雷射成本的部份===================================================

        # 拿DATA
    bending = connect_to_mysql('bending','svfs','2020-05-01 00:00:00','2020-08-30 23:59:59') # 取得折床data  要選開始跟結束日期
    bom_bending = connect_to_mysql('bending','smbsource') # 取得BOM表

        # 處理DATA及異常值
    bending_data = pre_processing('bending', bending)  # 把折床data丟進去做前處理，做完前處理後的折床DATA
    bending_data_with_name = merge_from_bom(bending_data,bom_bending)
    original_bending_data = deal_with_abnormal('bending',bending_data_with_name)

        # 原生DATA這邊就可以直接匯出了
    #original_bending_data.to_csv('testing_original_bending_data0831-0904.csv', index=False, header=True, encoding='utf_8_sig')

        # 開始加上各種額外資訊，去算它的耗時及成本
        # 1.加上薪資表
    bending_data_with_amount_wage = get_wage_table('bending', original_bending_data)
    almost_completed_bending_data = divied_comsumption_cost('bending', bending_data_with_amount_wage)

        # 2.最後一步算出單一折床子件成本
    real_bending_cost = cost_per_one('bending', almost_completed_bending_data,bom_bending)

        # 匯出折床單一子件成本
    #export_to_excel(real_bending_cost, '2折床_最早-0830')

# ===================================================以上為折床成本的部份===================================================

        # 拿DATA
    welding = connect_to_mysql('welding','welding_report','2020-05-01 00:00:00','2020-08-30 23:59:59') # 取得焊接data  要選開始跟結束日期
    worker_name = connect_to_mysql('welding', 'welding_report2') # 這邊是拿工人的名字

        # 處理DATA及異常值
    welding_data = pre_processing('welding', welding)  # 把雷射data丟進去做前處理，做完前處理後的雷射DATA
    welding_data_with_name = merge_from_worker_name(welding_data, worker_name) #加上工人名字的焊接DATA
    welding_data_with_name_time = merge_from_welding_standard_time(welding_data_with_name)  # 加上標準工時去算的DATA
    original_welding_data = extra_processing_for_welding_data(welding_data_with_name_time)  # 幫剛剛加進來的標準工時去算週總耗時

        # 原生DATA這邊就可以直接匯出了
    # original_welding_data.to_csv('original_welding_data0531-0627.csv', index=False, header=True, encoding='utf_8_sig')

        # 開始加上各種額外資訊，去算它的耗時及成本
        # 1.加上薪資表
    welding_data_with_name_wage = get_wage_table('welding', original_welding_data)
    almost_completed_welding_data = divied_comsumption_cost('welding', welding_data_with_name_wage)

        # 2.最後一步算出單一折床子件成本
    real_welding_cost = cost_per_one('welding', almost_completed_welding_data)

        # 匯出折床單一子件成本
    #export_to_excel(real_welding_cost, '3焊接_最早-0830')

# ===================================================以上為焊接成本的部份===================================================

    material_cost = get_material_data_and_bom()  # 這個是拿會計給我的BOM，已經暫時把料錢算好了

    # 這邊臨時把噴漆跟組立的成本拉進來拼在一起
    real_lacquer_cost = get_lacquer_and_combination_table('lacquer') # 基本上這邊之後要砍掉，要幫它們寫DEF，現在只是暫時這樣撈資料而已
    real_combination_cost = get_lacquer_and_combination_table('combination') # 基本上這邊之後要重砍，要幫它們寫DEF，現在只是暫時這樣撈資料而已

    # 這邊是把BOM+料錢+排版子件成本+雷射子件成本+折床子件成本+焊接子件成本
    merged_cost_table = merge_these_table_with_bom(material_cost, real_typesetting_cost, real_laser_cost, real_bending_cost, real_welding_cost,real_lacquer_cost,real_combination_cost,bom_laser)
    merged_cost_table2 = outsourcing(merged_cost_table)
    export_to_excel(merged_cost_table2, '0907_Bom_with_cost_from_earliest_to_0830')


# ===================================================以上為料錢及合併的計算的部份===================================================
    #print(get_electricity_table(which_factory=1,week=23))

    #almost_completed_laser_data
    #almost_completed_bending_data
    #almost_completed_welding_data

    end= time.time()
    print(end-start)




