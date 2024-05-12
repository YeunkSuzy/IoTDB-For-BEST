import IoTDB_parameter_get
import iotdb
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
import pymysql
from pymysql import OperationalError, MySQLError
import time
import datetime

#Datetime,Long转换
def convert_datetime_to_long(dt):
    dt_start = datetime.datetime(1970, 1, 1)
    to_now = dt - dt_start
    timestamp = int(to_now.total_seconds())
    return timestamp * 1000 - 28800000

#从文件中获取last_id
def get_last_id():
    try:
        # 尝试从文件中读取最后一个处理的ID
        with open(last_id_file, 'r') as file:
            last_id = int(file.read())
    except FileNotFoundError:
        # 如果文件不存在，则返回0作为初始值
        last_id = 0
    except ValueError:
        # 如果文件中的内容无法解析为整数，则返回0作为初始值
        last_id = 0

    return last_id

#在文件中写入last_id
def save_last_id(last_id):
    # 将最后一个处理的ID保存到文件中
    with open(last_id_file, 'w') as file:
        file.write(str(last_id))

# 通过last_id获取Mysql更新数据
def get_new_inserted_data(last_id):
    try:
        query = f"SELECT * FROM {table_name} WHERE id >= {last_id}"
        # 查询大于最后一次ID的新插入数据
        cursor.execute(query)
        new_data = cursor.fetchall()
        rownum_data = cursor.rowcount
        return rownum_data, new_data

    except (OperationalError, MySQLError) as e:
        print("MySQL连接错误:", e)
        return []

#IoTDB连接信息
ip = "xxx"
port_ = "6667"
username_ = "xxx"
password_ = "xxx"
session = Session(ip, port_, username_, password_)
session.open(False)
# 创建 MySQL 连接
mysql_db = pymysql.connect(
    host="xxx",
    port=3306,
    user="xxx",
    password="xxx",
    database="engdata"
)

# 文件路径用于存储最后一个处理的ID
last_id_file = 'last_id.txt'
# 连接到MySQL数据库
cursor = mysql_db.cursor()   #创建游标连接数据库

# #获取Table_Name
# table_name = input("Please enter table name:")
table_name = "cryogenic_data"

# 获取最后一个处理的ID
last_id = get_last_id()


# 循环监听数据变化
while True:
    try:
        # 根据last_id来获取新写入的数据
        rownum_data, new_data = get_new_inserted_data(last_id)

        # 获取IoTDB各项参数，并将他们按device_size分片
        device_size = 60
        measurements_lsts, data_type_lsts = IoTDB_parameter_get.parameter_choice(table_name, cursor)

        measurements_lst = IoTDB_parameter_get.slicing_list(measurements_lsts, device_size)
        data_type_lst = IoTDB_parameter_get.slicing_list(data_type_lsts, device_size)
        devices = IoTDB_parameter_get.devices_get(measurements_lsts, device_size, table_name)

        #Mysql有新数据更新，准备插入IoTDB
        if new_data:

            #准备插入并计时
            print(f"This Update have {rownum_data} row New Data. Prepare to insert to IoTDB!")
            start = time.perf_counter()
            iter_num = 0  #计数 第几行

            #按行遍历Mysql中的更新数据
            for row in new_data:

                #若此行时间为空，则跳过此行
                if row[IoTDB_parameter_get.timerow_get(table_name)] is None or row[IoTDB_parameter_get.timerow_get(table_name)] == " ":
                    continue

                #将时间戳转为long，将value转为元组并分片
                timestamp = convert_datetime_to_long(row[IoTDB_parameter_get.timerow_get(table_name)])
                values = tuple(row)
                value = IoTDB_parameter_get.slicing_list(values, device_size)

                #循环 按device_size进行插入
                for i in range(len(devices)):

                    # 将此个device的参数全部列表化
                    measurement = list(measurements_lst[i])
                    data_type = list(data_type_lst[i])
                    val = list(value[i])

                    # 开始插入
                    j = 0
                    while j < len(val):

                        # 调整mysql中的值，若类型为datetime.datetime则将其转化为string类型
                        if measurements_lst[i][j] == "time":
                            measurement[j] = "curr_time"
                        if isinstance(value[i][j], datetime.datetime):
                            val[j] = value[i][j].strftime("%Y-%m-%d %H:%M:%S")
                            j += 1
                        elif value[i][j] is None or value[i][j] == "":
                            measurement.pop(j)
                            data_type.pop(j)
                            val.pop(j)
                        else:
                            j += 1

                    # IoTDB插入函数
                    session.insert_record(
                        f"root.{table_name}.{devices[i]}", timestamp, measurement, data_type, val
                    )

                #计数+1 跟踪写入进度
                iter_num += 1
                #*的显示
                a = "*" * int(iter_num * 100 / rownum_data)
                #.的显示
                b = "." * int((rownum_data - iter_num) * 100/rownum_data)
                #百分比
                c = (iter_num / rownum_data) * 100

                taketime = time.perf_counter() - start

                print("\r{:^3.0f}%[{}->{}]{:.2f}s".format(c, a, b, taketime), end="")

        #Mysql无新数据更新
        else:
            print("No New Data!")

        print(f"\nThe insertion is completed! {rownum_data} time points are inserted. It takes {taketime} second.\n ")

        # 更新最后一次ID
        last_id = max(row[0] for row in new_data)
        save_last_id(last_id)

        break

    except (KeyboardInterrupt, SystemExit):
        break


cursor.close()

mysql_db.close()

session.close()
