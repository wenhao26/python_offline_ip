#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import time
import argparse
import requests
import sqlite3
import concurrent.futures

raw_files = [
    'delegated-afrinic-latest.txt',
    'delegated-apnic-latest.txt',
    'delegated-arin-extended-latest.txt',
    'delegated-lacnic-latest.txt',
    'delegated-ripencc-latest.txt',
]
count = 0


def root_path():
    """
    根目录
    :return:
    """
    return os.getcwd()


def current_date():
    """
    当前日期
    :return:
    """
    return time.strftime('%Y%m%d', time.localtime())


def create_dir(dir_path):
    """
    创建目录
    :param dir_path:
    :return:
    """
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return dir_path


def get_ipv4_data_file():
    """
    ipv4数据文件
    :return:
    """
    return root_path() + '/data/db/ipv4.txt'


def get_ip_db_file():
    """
    ip数据文件
    :return:
    """
    return root_path() + '/data/db/ip.db'


def get_list_value(lst, index, default_value=None):
    """
    获取列表中指定索引位置的元素，如果索引不存在则返回默认值
    :param lst:
    :param index:
    :param default_value:
    :return:
    """
    try:
        return lst[index]
    except IndexError:
        return default_value


def ip2long(ip):
    """
    ip地址转长整型
    :param ip:
    :return:
    """
    try:
        ip_list = [int(i) for i in ip.split('.')]
        if len(ip_list) != 4:
            raise ValueError("Invalid IP format")
        for i in ip_list:
            if i < 0 or i > 255:
                raise ValueError("Invalid IP format")
        return (ip_list[0] << 24) + (ip_list[1] << 16) + (ip_list[2] << 8) + ip_list[3]
    except (ValueError, TypeError):
        return None


def long2ip(ip_long):
    """
    将长整型IP表示转换为IP地址字符串（适用于IPv4）
    """
    return '.'.join([str((ip_long >> (8 * i)) & 0xFF) for i in range(3, -1, -1)])


def fetch_url_content(url):
    """
    获取URL内容
    :param url:
    :return:
    """
    print(f"[INFO] 正在同步`{url}`数据...")
    result_dict = {}
    try:
        response = requests.get(url)
        response.raise_for_status()
        filename = os.path.basename(url)
        result_dict[filename] = response.text
    except requests.RequestException as e:
        print(f"[ERROR] 获取{url}内容失败，错误原因: {e}")

    return result_dict


def raw_filter_ipv4(raw_file, save_file):
    """
    原始数据筛选出ipv4
    :param raw_file:
    :param save_file:
    :return:
    """
    global count

    print('[INFO] 开始解析文件内容，逐行处理...')

    # 读取文件
    file = open(raw_file, 'rb')
    while True:
        line = file.readline()
        if not line:
            break

        # 逐行分析
        line_content = line.strip().decode('utf-8')
        if line_content.find('#') == 0 or 'ipv4' not in line_content.lower():
            continue

        # 解析内容
        content_list = line_content.split('|')
        organization = get_list_value(content_list, 0, '')
        country = get_list_value(content_list, 1, '')
        ip_type = get_list_value(content_list, 2, '')
        ip = get_list_value(content_list, 3, '')
        length = get_list_value(content_list, 4, 0)
        date = get_list_value(content_list, 5, '')
        status = get_list_value(content_list, 6, '')
        if ip_type != 'ipv4' or (status != 'assigned' and status != 'allocated'):
            continue

        ip_start = ip2long(ip)
        ip_end = ip_start + int(length) - 1

        # 写入文件
        save_dir = os.path.dirname(save_file)
        if not os.path.exists(save_dir):
            create_dir(save_dir)

        row = f"{country},{ip_start},{ip_end},{ip},{long2ip(ip_end)}"
        with open(save_file, 'a', encoding='utf-8') as f:
            f.write(row + "\n")
            count += 1

        print(f"[INFO]`{line_content}`已进行解析写入...")
    file.close()

    print('----------------------------------------')
    time.sleep(1)


def pull():
    """
    拉取原始数据
    :return:
    """
    print('[INFO] >>> 拉取原始数据 <<<')
    # raw_dir = root_path() + '/data/raw/' + current_date()
    raw_dir = root_path() + '/data/raw/latest'
    create_dir(raw_dir)

    url_dict = {
        'delegated-apnic-latest': 'https://ftp.apnic.net/stats/apnic/delegated-apnic-latest',
        'delegated-arin-extended-latest': 'https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest',
        'delegated-afrinic-latest': 'https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-latest',
        'delegated-lacnic-latest': 'https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-latest',
        'delegated-ripencc-latest': 'https://ftp.ripe.net/ripe/stats/delegated-ripencc-latest'
    }

    # 串行方式
    # for name, url in url_dict.items():
    #     print(f"[INFO] 正在同步`{url}`数据...")
    #     try:
    #         response = requests.get(url)
    #         response.raise_for_status()
    #         content = response.text
    #         save_path = f"{raw_dir}/{name}.txt"
    #         with open(save_path, 'w', encoding='utf-8') as f:
    #             f.write(content)
    #             print(f"[INFO] `{url}`内容写入完成\n")
    #     except requests.RequestException as e:
    #         print(f"[ERROR] 获取{url}内容失败，错误原因: {e}")

    # 并行方式
    urls = []
    for name, url in url_dict.items():
        urls.append(url)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(fetch_url_content, urls))

    if results:
        for result in results:
            for name, content in result.items():
                save_path = f"{raw_dir}/{name}.txt"
                with open(save_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                    print(f"[INFO] `{url_dict[name]}`内容写入完成\n")
    else:
        print('[INFO] 拉取数据集为空')


def create():
    """
    生成ipv4数据并保存为本地文件
    :return:
    """
    print('[INFO] >>> 生成ipv4数据并保存为本地文件 <<<')
    ipv4_data_file = get_ipv4_data_file()

    # 先移除原有的ipv4数据文件
    if os.path.exists(ipv4_data_file):
        try:
            os.remove(ipv4_data_file)
            print(f"[INFO] 文件{ipv4_data_file}已成功删除")
        except FileNotFoundError:
            print(f"[ERROR] {ipv4_data_file}不存在，无法删除")
            exit(0)
        except PermissionError:
            print(f"[ERROR] 没有权限删除文件 {ipv4_data_file}")
            exit(0)
        except OSError as e:
            print(f"[ERROR] 删除文件 {ipv4_data_file}时出现其他错误: {e}")
            exit(0)
    else:
        print(f"[INFO] 文件{ipv4_data_file}不存在，无需删除")

    time.sleep(2)

    for file in raw_files:
        print(f"[INFO] 装载文件：{file}")
        raw_filter_ipv4(root_path() + '/data/raw/latest/' + file, ipv4_data_file)

    print(f"[INFO]本次任务处理的行数：{count}")


def save():
    """
    将ipv4文本数据保存为sqlite数据文件
    :return:
    """
    print('[INFO] >>> 将ipv4文本文件的数据保存为sqlite3数据库文件 <<<')

    ipv4_data_file = get_ipv4_data_file()
    ip_db_file = get_ip_db_file()
    if os.path.exists(ipv4_data_file) is False or os.path.exists(ip_db_file) is False:
        print(f"[ERROR] `{ipv4_data_file}`或`{ip_db_file}`不存在")
        exit(0)

    # 连接sqlite3
    connect = sqlite3.connect(ip_db_file)
    cursor = connect.cursor()

    # 更新前，将原有的`ip_area`表清空
    print('[INFO] 正在清空`ip_area`表，清空数据需要点时间，请勿退出...')
    try:
        cursor.execute('delete from ip_area')
        connect.commit()

        print('[INFO] `ip_area`表已清空，开始进行数据写入操作：')
        time.sleep(2)
    except sqlite3.Error as e:
        print('[ERROR] 数据库错误：', e)
        exit(0)

    # cursor.execute("select * from ip_area")
    # results = cursor.fetchall()
    # print(results)

    # 读ipv4源数据，写入sqlite
    row_count = 0
    insert_values = ''

    file = open(ipv4_data_file, 'rb')
    while True:
        line = file.readline()
        if not line:
            break

        # 解析数据
        line_content = line.strip().decode('utf-8')
        content_list = line_content.split(',')
        country = get_list_value(content_list, 0, '')
        start_ip_no = get_list_value(content_list, 1, '')
        end_ip_no = get_list_value(content_list, 2, '')
        start_ip = get_list_value(content_list, 3, '')
        end_ip = get_list_value(content_list, 4, '')

        row_count += 1
        if row_count < 10000:
            insert_values += f"('{country}','{start_ip_no}','{end_ip_no}','{start_ip}','{end_ip}'),"
        else:
            # 插入数据
            try:
                insert_values = insert_values.rstrip(',')
                cursor.execute(
                    f"insert into ip_area(country,start_ip_no,end_ip_no,start_ip,end_ip) values {insert_values}")
                connect.commit()
                insert_values = ''
                row_count = 0
                print('[INFO] batch1：写入成功')
            except sqlite3.Error as e:
                connect.rollback()
                print('[ERROR] 数据库错误：', e)
                exit(0)
    file.close()

    # 防止不满足批次阈值，漏写数据
    if len(insert_values):
        try:
            insert_values = insert_values.rstrip(',')
            cursor.execute(
                f"insert into ip_area(country,start_ip_no,end_ip_no,start_ip,end_ip) values {insert_values}")
            connect.commit()
            print('[INFO] batch2：写入成功')
        except sqlite3.Error as e:
            connect.rollback()
            print('[ERROR] 数据库错误：', e)
            exit(0)

    # 关闭游标和连接
    cursor.close()
    connect.close()


def search(ip):
    """
    查询IP信息
    :param ip:
    :return:
    """
    print('[INFO] >>> 查询IP信息 <<<')

    ip_no = ip2long(ip)
    if ip_no is False:
        print(f"{ip}不合法")
        exit(0)

    ip_db_file = get_ip_db_file()
    if os.path.exists(ip_db_file) is False:
        print(f"[ERROR] `{ip_db_file}`不存在")
        exit(0)

    # 连接sqlite3
    connect = sqlite3.connect(ip_db_file)
    cursor = connect.cursor()

    cursor.execute(f"select * from ip_area where {ip_no} >= start_ip_no and {ip_no} <= end_ip_no limit 1")
    result = cursor.fetchone()
    if result:
        print(f"查询结果：\n - id：{result[0]}\n - country：{result[1]}"
              f"\n - start_ip_no：{result[2]}\n - end_ip_no：{result[3]}"
              f"\n - start_ip：{result[4]}\n - end_ip：{result[5]}")
    else:
        print(f"查询结果：\n - 暂无`{ip}`相关信息")


def main():
    parser = argparse.ArgumentParser(description="Process some commands.")

    # 添加参数
    # parser.add_argument(
    #     'command',
    #     choices=['pull', 'create', 'save', 'search'],
    #     help='Choose a method to call: `pull`,`create`,`save` or `search`'
    # )
    subparsers = parser.add_subparsers(dest='command', required=True)
    subparsers.add_parser('pull', help='拉取数据')
    subparsers.add_parser('create', help='创建数据')
    subparsers.add_parser('save', help='保存数据')
    search_parser = subparsers.add_parser('search', help='查询数据')
    search_parser.add_argument('ip', type=str, help='用于搜索的IP地址，例如“127.0.0.1”')

    args = parser.parse_args()
    if args.command == 'pull':
        pull()
    elif args.command == 'create':
        create()
    elif args.command == 'save':
        save()
    elif args.command == 'search':
        if len(args.ip) == 0:
            print('请输入查询的IP地址')
        search(args.ip)
    else:
        print('无效的指令')


if __name__ == '__main__':
    start_time = time.time()

    main()

    end_time = time.time()
    total_seconds = end_time - start_time
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)
    formatted_time = "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds)
    print(f"程序执行时间: {formatted_time}")
