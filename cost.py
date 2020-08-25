import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime
from datetime import timedelta
import time
import numpy as np

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
            cursor.execute("SELECT product_id,parent_id \
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
            print('successfully got the bending data !')
            return data_name


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
    df_parents.columns = ['子件料號', '母件料號']

    data_merged = pd.merge(data, df_parents, how='left')

    return data_merged


def merge_from_bom(bending_data,parent): # 參數:(前處理後的折床DATA,BOM表)  前處理後的折床DATA加上子件料號跟數量,母件名稱

    # 缺母件名稱的折床報表
    df_parts_log = pd.DataFrame(bending_data)
    df_parts_log.columns = ['機台', '子件料號', '週數', '週總耗時', '起始時間', '結束時間', '耗時', '完成數量', '失敗數量']

    # 從bom去撈母件名稱
    df_parent = pd.DataFrame(parent)
    df_parent.columns = ['子件料號', '母件料號', '數量']

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
                        real_data.iloc[parent[0], 11] = '*' + parent[1]

                    else:  # 就都取自己就對了
                        parent = [index, row[9]]
                        real_data.iloc[parent[0], 11] = '*' + parent[1]
        return real_data

    elif task=='bending':

        for index, row in real_data.iterrows():

            if type(row[9]) == float:

                if len(row[1].split('-')) >= 2:
                    parent = [index, row[1].split('-')[0]]
                    real_data.iloc[parent[0], 9] = '*' + parent[1]

                else:
                    parent = [index, row[1]]
                    real_data.iloc[parent[0], 9] = '*' + parent[1]

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

        if row[12] >= 1:  # 處理排版圖子件數為0的時候
            pass
        else:
            real_data_no2.iloc[index, 12] = 1

    return real_data_no2


def get_wage_table(task,data): # 參數:(處理完後的雷射DATA)  加上薪資表欄位，準備算

    wage_table = pd.read_excel('-歷史薪水.xlsx')  # 讀取薪資表

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
            divided_time.append(row[6]*row[10]/row[12])
        data['分攤耗時']=divided_time

        divided_cost=[]
        for index ,row in data.iterrows():
            divided_cost.append(row[13]*row[14]/row[3])
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


def cost_per_one(task,data): # 參數:(處理完後的雷射DATA)  最後算出單一雷射子件成本

    if task == 'laser':

        true_laser_cost = []
        grouped = data.groupby(['子件料號', '母件料號'])  # 用groupby的方法處理
        agg = grouped['子件數量', '分攤成本'].agg(np.sum)
        laser_cost = pd.DataFrame(agg)
        laser_cost.reset_index(inplace=True)

        for i, q in zip(laser_cost['分攤成本'], laser_cost['子件數量']):
            true_laser_cost.append(i / q)

        laser_cost['子件雷射單一成本'] = true_laser_cost
        return laser_cost

    elif task == 'bending':

        true_bending_cost = []
        grouped = data.groupby(['子件料號', '母件料號'])  # 用groupby的方法處理
        agg = grouped['完成數量', '總成本'].agg(np.sum)
        bending_cost = pd.DataFrame(agg)
        bending_cost.reset_index(inplace=True)

        for i, q in zip(bending_cost['總成本'], bending_cost['完成數量']):
            true_bending_cost.append(i / q)

        bending_cost['子件折床單一成本'] = true_bending_cost
        return bending_cost

    if task == 'welding':

        true_welding_cost = []
        grouped = data.groupby('母件料號')  # 用groupby的方法處理
        agg = grouped['實際生產數量', '總成本'].agg(np.sum)
        welding_cost = pd.DataFrame(agg)
        welding_cost.reset_index(inplace=True)

        for i, q in zip(welding_cost['總成本'], welding_cost['實際生產數量']):
            true_welding_cost.append(i / q)

        welding_cost['母件焊接單一成本'] = true_welding_cost
        return welding_cost


def abnormal_parents(data): #參數:(前面產出來的雷射DATA)     雷射生產了一些出貨表不用出的東西，經過這個def去比對，找出異常data
    abnormal = pd.read_excel('檢查母件異常，該要有但雷射沒有的.xls')  # 讀取出貨表
    abnormal.rename(columns={'產品編號': '母件料號'}, inplace=True)
    abnormal.drop_duplicates(subset='母件料號', keep='first', inplace=True)
    abnormal_data = pd.merge(data, abnormal, how='left')
    abnormal_data_list=abnormal_data[abnormal_data['單據號碼'].isnull()]
    abnormal_data_list.to_csv('雷射生產異常data.csv', index=False, mode='w', header=True, encoding='utf_8_sig')


def export_to_csv(data,rename): #參數:(最後匯出成CSV檔的話都是來這一步)
    data.to_csv('{}.csv'.format(rename), index=False, mode='w', header=True, encoding='utf_8_sig')


if __name__ == '__main__':

    '''
        #拿DATA
    laser = connect_to_mysql('laser','svfs','2020-05-31 00:00:00','2020-06-27 23:59:59') #取得雷射data  要選開始跟結束日期
    typesetting_laser = connect_to_mysql('laser','qrcode_label') #取得排版圖data
    bom_laser = connect_to_mysql('laser','smbsource') #取得bom表data

        #處理DATA及異常值
    laser_data = pre_processing('laser', laser) #把雷射data丟進去做前處理，做完前處理後的雷射DATA
    laser_data_with_name = merge_from_typesetting_and_bom(laser_data,typesetting_laser,bom_laser) #把子件、數量、母件通通拉了進來
    original_laser_data = deal_with_abnormal('laser',laser_data_with_name) #處理異常數據:子件數量變1, 用子件去幫母件改名

        #原生DATA這邊就可以直接匯出了
    #original_laser_data.to_csv('original_laser_data0531-0627.csv', index=False, mode='w', header=True, encoding='utf_8_sig')

        #開始加上各種額外資訊，去算它的耗時及成本
        #1.這個是統計它的排版圖的子件總數量,且跟前面data merge起來
    laser_data_with_amount = amount_of_typesetting(typesetting_laser,original_laser_data)
        #2.加上薪資表
    laser_data_with_amount_wage = get_wage_table('laser',laser_data_with_amount)
    almost_completed_laser_data = divied_comsumption_cost('laser',laser_data_with_amount_wage)
        #3.最後一步算出單一雷射子件成本
    real_laser_cost = cost_per_one('laser',almost_completed_laser_data)
        #4.這邊是會計叫我做的查看雷射異常DATA(有需要再用)  記得要去deal_with_abnormal()把它的星號除掉，比較方便
    #abnormal_parents(real_laser_cost)

        #匯出雷射單一子件成本
    export_to_csv(real_laser_cost, '雷射0531-0627')
#===================================================以上為雷射成本的部份===================================================
    
        #拿DATA
    bending = connect_to_mysql('bending','svfs','2020-05-31 00:00:00','2020-06-27 23:59:59') #取得折床data  要選開始跟結束日期
    bom_bending = connect_to_mysql('bending','smbsource') #取得BOM表

        #處理DATA及異常值
    bending_data = pre_processing('bending', bending)  # 把折床data丟進去做前處理，做完前處理後的折床DATA
    bending_data_with_name = merge_from_bom(bending_data,bom_bending)
    original_bending_data = deal_with_abnormal('bending',bending_data_with_name)

        # 原生DATA這邊就可以直接匯出了
    #original_bending_data.to_csv('original_bending_data0531-0627.csv', index=False, mode='w', header=True, encoding='utf_8_sig')

        # 開始加上各種額外資訊，去算它的耗時及成本
        #1.加上薪資表
    bending_data_with_amount_wage = get_wage_table('bending', original_bending_data)
    almost_completed_bending_data = divied_comsumption_cost('bending', bending_data_with_amount_wage)

        #2.最後一步算出單一折床子件成本
    real_bending_cost = cost_per_one('bending', almost_completed_bending_data)

        #匯出折床單一子件成本
    export_to_csv(real_bending_cost, '折床0531-0627')

# ===================================================以上為折床成本的部份===================================================
    '''
        #拿DATA
    welding = connect_to_mysql('welding','welding_report','2020-05-31 00:00:00','2020-06-27 23:59:59') #取得焊接data  要選開始跟結束日期
    worker_name = connect_to_mysql('welding', 'welding_report2') #這邊是拿工人的名字

        # 處理DATA及異常值
    welding_data = pre_processing('welding', welding)  # 把雷射data丟進去做前處理，做完前處理後的雷射DATA
    welding_data_with_name = merge_from_worker_name(welding_data, worker_name) #加上工人名字的焊接DATA
    welding_data_with_name_time = merge_from_welding_standard_time(welding_data_with_name)  #加上標準工時去算的DATA
    original_welding_data = extra_processing_for_welding_data(welding_data_with_name_time)  #幫剛剛加進來的標準工時去算週總耗時

        # 原生DATA這邊就可以直接匯出了
    #original_welding_data.to_csv('original_welding_data0531-0627.csv', index=False, mode='w', header=True, encoding='utf_8_sig')

        # 開始加上各種額外資訊，去算它的耗時及成本
        # 1.加上薪資表
    welding_data_with_name_wage = get_wage_table('welding', original_welding_data)
    almost_completed_welding_data = divied_comsumption_cost('welding', welding_data_with_name_wage)

        # 2.最後一步算出單一折床子件成本
    real_welding_cost = cost_per_one('welding', almost_completed_welding_data)

        #匯出折床單一子件成本
    export_to_csv(real_welding_cost, '焊接0531-0627')

