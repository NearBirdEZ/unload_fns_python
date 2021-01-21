#!/usr/bin/env python3

from connections import Connections
import json
import datetime as dt
import calendar
import os
from math import ceil
import zipfile
from threading import Thread, Lock
import requests
from bs4 import BeautifulSoup
#import notify2


def get_version():
    v = requests.get('https://github.com/NearBirdEZ/unload_fns_python/blob/main/properties')
    soup = BeautifulSoup(v.text, 'html.parser')
    version_online = soup.find('td', class_='blob-code blob-code-inner js-file-line').get_text()
    with open('../properties', 'r') as vars:
        for line in vars:
            if line.strip().startswith('version'):
                local_version = line.strip()
                break
    return local_version == version_online


class UnloadFns:

    def __init__(self, request_number, threads, date_in, inn_string, rnm_string, gui):
        self.request = request_number
        self.threads = threads
        self.date_list = self.__division_by_month(date_in)
        self.inn_string = inn_string
        self.rnm_string = rnm_string
        self.connect = Connections('../properties')
        self.__job_folder()
        self.count_fn = 0
        self.bar = None
        self.count_bar = 0
        self.lock = Lock()
        self.gui = gui


    def __division_by_month(self, date_in):
        # Разбиваем даты помесячно
        time1 = date_in[0]
        time2 = date_in[1] + dt.timedelta(hours=23, minutes=59, seconds=59)
        date_list_timestamp = []
        while time1 + dt.timedelta(days=1) < time2:
            days_in_month = dt.timedelta(
                days=calendar.monthrange(time1.year, time1.month)[1] - time1.day + 1) - dt.timedelta(seconds=1)
            if time1 + days_in_month < time2:
                gap_time = time1 + days_in_month
            else:
                gap_time = time1 + (time2 - time1)
            date_list_timestamp.append([time1, gap_time])
            time1 = gap_time + dt.timedelta(seconds=1)
        return date_list_timestamp

    def __job_folder(self):
        """Создаем рабочую директорию и переходим в нее"""
        if not os.path.exists(f"../unload/"):
            os.mkdir(f"../unload/")

        if not os.path.exists(f"../unload/{self.request}/"):
            os.mkdir(f"../unload/{self.request}/")
        os.chdir(f"../unload/{self.request}/")

    def collect_rnm_inn(self):
        """Формируем запрос SQL и получаем таблицу вида RNM - INN"""
        request = f"select kkt.register_number_kkt, company.company_inn from kkt inner join company on company." \
                  f"id=kkt.company_id  where company.company_inn in ({self.inn_string}) {self.rnm_string}"
        rnm_inn_list = self.connect.to_sql(request)
        return rnm_inn_list

    def collect_fn(self, rnm):
        """По полученым РНМ уточняем все установленные ФНы"""
        query = '{"size": 0,"query" : {"bool" : {"must" : [{"term" : {"requestmessage.kktRegId.raw" : "%s"}}]}},' \
                '"aggs": {"fsIds": {"terms": {"field": "requestmessage.fiscalDriveNumber.raw","size": 500000}}}}' % (
                    rnm)
        fn_list = self.connect.to_elastic(query)['aggregations']['fsIds']['buckets']
        return fn_list

    def get_dict_inn_rnm_fn(self):
        rnm_inn_list = self.collect_rnm_inn()
        self.bar['maximum'] = len(rnm_inn_list) * 2
        three_inn_rnm_fn_dict = {}
        for rnm, inn in rnm_inn_list:
            self.next_bar()
            for fn in self.collect_fn(rnm):
                fn = fn['key']
                if three_inn_rnm_fn_dict.get(inn):
                    three_inn_rnm_fn_dict[inn].append((rnm, fn))
                else:
                    three_inn_rnm_fn_dict[inn] = [(rnm, fn)]
                self.count_fn += 1
            self.next_bar()
        self.count_bar = 0
        return three_inn_rnm_fn_dict

    def min_max_fd(self, rnm, fn, start_date, end_date):
        """Получаем минимальный и максимальные ФД в периоде относительно РНМ и ФН"""
        stats_fd_request = '{"query" : {"bool" : {"filter" : {"bool" : {"must" : ' \
                           '[{"term" : {"requestmessage.fiscalDriveNumber.raw" : "%s" }},' \
                           '{"term" : {"requestmessage.kktRegId.raw" : "%s" }}, ' \
                           '{"range" : {"requestmessage.dateTime" : {"gte" : "%d", "lte" : "%d" }}}]}}}}, ' \
                           '"aggs" : {"stats" : { "stats" : { "field" : "requestmessage.fiscalDocumentNumber" }}}}' % (
                               fn, rnm, start_date, end_date)
        stats = self.connect.to_elastic(stats_fd_request)['aggregations']['stats']
        max_fd = stats['max']
        min_fd = stats['min']
        return min_fd, max_fd

    def download_json(self, inn, rnm, fn, min_fd, max_fd, num):
        """Основной скрипт выгрузки
        Формируется запрос согласно максимального и минимального ФД по РНМ:ФН
        Выгружаются по всем необходимым индексам
        Флаг необходим для запуска функции архивирования"""
        flag = False

        index_list = ['receipt.*',
                      'open_shift',
                      'close_shift',
                      'fiscal_report',
                      'fiscal_report_correction',
                      'bso',
                      'bso_correction',
                      'current_state_report',
                      'close_archive']

        delta = max_fd - min_fd
        iteration = ceil(delta / 10000)
        for type_fd in index_list:
            rec_list = []
            for _ in range(iteration):
                data = '{"from" : 0, "size" : 10000, "_source" : {"includes" : ["requestmessage.*"]}, ' \
                       '"query" : {"bool" : {"filter" : {"bool" : { "must" : ' \
                       '[{"term" : {"requestmessage.fiscalDriveNumber.raw" : "%s"}}, ' \
                       '{"term" : {"requestmessage.kktRegId.raw" : "%s"}},' \
                       '{"range" : {"requestmessage.fiscalDocumentNumber" : {"gte" : %d, "lte" : %d }}}]}}}}, ' \
                       '"sort" : [{ "requestmessage.fiscalDocumentNumber" : { "order" : "asc"}}]}' % \
                       (fn, rnm, min_fd, max_fd)
                receipts = self.connect.to_elastic(data, type_fd)['hits']['hits']
                rec_list += receipts
                min_fd += 10000
            if rec_list:
                flag = True
                self.write_json(rec_list, inn, rnm, fn, type_fd, num)
            """Возвращаем минимальное значение ФД"""
            min_fd = max_fd - delta
        return flag

    def write_json(self, data_json, inn, rnm, fn, type_fd, num):
        """Записывам данные в формате json"""
        try:
            """В связи с тем, что несколько потоков пытаются создать папку, if не успевает. lock не вижу смысла"""
            if not os.path.exists(f"./{inn}/"):
                os.mkdir(f"./{inn}/")
        except FileExistsError:
            pass
        try:
            if not os.path.isdir(f"./{inn}/{rnm}.{fn}/"):
                os.mkdir(f"./{inn}/{rnm}.{fn}/")
        except FileExistsError:
            pass

        with open(f'./{inn}/{rnm}.{fn}/{rnm}.{fn}.{type_fd}_{num}.json', 'w', encoding='utf-8') as file:
            receipts = [rec['_source']['requestmessage'] for rec in data_json]
            json.dump(receipts,
                      file,
                      indent=4,
                      ensure_ascii=False,
                      sort_keys=False)

    def start_threading(self, inn, rnm_fn_list, unload_flag):
        tread_list = []
        for i in range(self.threads):
            if unload_flag:
                t = Thread(target=self.thread_job_month, args=(i, inn, rnm_fn_list))
            else:
                t = Thread(target=self.thread_job_rnm, args=(i, inn, rnm_fn_list))
            t.start()
            tread_list.append(t)
        for i in range(self.threads):
            tread_list[i].join()


    def next_bar(self):
        self.lock.acquire()
        self.count_bar += 1
        self.bar['value'] = self.count_bar
        self.gui.update()
        self.lock.release()

    def thread_job_month(self, num_thread, inn, rnm_fn_list):
        for rnm, fn in rnm_fn_list:
            self.next_bar()
            for i in range(num_thread, len(self.date_list), self.threads):
                start_date = int(self.date_list[i][0].timestamp())
                end_date = int(self.date_list[i][1].timestamp())
                min_fd, max_fd = self.min_max_fd(rnm, fn, start_date, end_date)
                if min_fd and max_fd:
                    min_fd, max_fd = int(min_fd), int(max_fd)
                    if self.download_json(inn, rnm, fn, min_fd, max_fd, i):
                        self.zipped(inn, rnm, fn, self.date_list[i], i)
            self.next_bar()

    def thread_job_rnm(self, num_thread, inn, rnm_fn_list):
        for i in range(num_thread, len(rnm_fn_list), self.threads):
            rnm = rnm_fn_list[i][0]
            fn = rnm_fn_list[i][1]
            self.next_bar()
            for dates in self.date_list:
                start_date = int(dates[0].timestamp())
                end_date = int(dates[1].timestamp())
                min_fd, max_fd = self.min_max_fd(rnm, fn, start_date, end_date)
                if min_fd and max_fd:
                    min_fd, max_fd = int(min_fd), int(max_fd)
                    if self.download_json(inn, rnm, fn, min_fd, max_fd, i):
                        self.zipped(inn, rnm, fn, dates, i)
            self.next_bar()

    def zipped(self, inn, rnm, fn, period, num):
        """Зипую папку с именем rnm.fn.period"""
        path = f'./{inn}/{rnm}.{fn}/'
        file_dir = os.listdir(path)
        with zipfile.ZipFile(f'{path}{rnm}.{fn}_{period[0].strftime("%Y_%m_%d")}-{period[1].strftime("%Y_%m_%d")}.zip',
                             mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            for file in file_dir:
                if file.endswith(f'_{num}.json'):
                    os.renames(os.path.join(path, file), os.path.join(f'{path}{num}/', file))
                    os.renames(os.path.join(f'{path}{num}/', file),
                               os.path.join(f'{path}{num}/', file.replace('.*', '').
                                            replace(f'_{num}', '')))
                    file = file.replace('.*', '').replace(f'_{num}', '')
                    add_file = os.path.join(f'{path}{num}/', file)
                    zf.write(add_file, file)
                    os.remove(add_file)
                    os.rmdir(f'{path}{num}')

    def final_zip(self):
        file_path = []
        sum_folder = 0
        sum_files = 0
        for root, dirs, files in os.walk('.'):
            file_path.append([os.path.join(root, file) for file in files])

        file_list = []
        for folder in file_path:
            weight_folder = 0
            if len(folder) == 0:
                continue
            for file in folder:
                weight_folder += os.path.getsize(file)
                sum_files += 1
            sum_folder += weight_folder
            file_list.append((weight_folder, folder))

        self.bar['maximum'] = sum_files
        limit_weight = 1403238553

        count_files = ceil(sum_folder / limit_weight)

        weight = 0
        name_file_list = []
        count = 0

        def zip_files(self, files_list, num):
            with zipfile.ZipFile(f'Выгрузка по заявке {self.request}{num}.zip', mode='w',
                                 compression=zipfile.ZIP_DEFLATED) as zipFile:
                for file in files_list:
                    zipFile.write(file)
                    file_dir = os.path.split(file)[0]
                    os.remove(file)
                    self.next_bar()
                    try:
                        os.removedirs(file_dir)
                    except OSError:
                        pass

        while True:
            if len(file_list) == 0:
                break
            for i in range(len(file_list)):
                if weight + file_list[i][0] < limit_weight:
                    weight += file_list[i][0]
                    name_file_list += file_list[i][1]
                    file_list.remove(file_list[i])
                if len(file_list) == 0 or weight + file_list[i][0] >= limit_weight:
                    count += 1

                    if count_files == 1:
                        num_zip = ''
                    else:
                        num_zip = f' № {count}'

                    zip_files(self, name_file_list, num_zip)
                    name_file_list.clear()
                    weight = 0
                break

    def analysis(self):
        count_month = len(self.date_list)
        flag = None

        if self.threads >= max(count_month, self.count_fn):
            if count_month >= self.count_fn:
                self.threads = count_month
                flag = True
            else:
                self.threads = self.count_fn
                flag = False
        elif self.threads < max(count_month, self.count_fn):
            if count_month >= self.count_fn:
                self.threads = ceil(count_month / ceil(count_month / self.threads))
                flag = True
            else:
                self.threads = ceil(self.count_fn / ceil(self.count_fn / self.threads))
                flag = False
        return flag

"""
def notify(request):
    # Оповещалка об окончании выгрузки
    notify2.init(f'Выгрузка по заявке {request}')
    n = notify2.Notification(f'Выгрузка по заявке № {request} завершилась.')
    n.set_urgency(notify2.URGENCY_NORMAL)
    n.show()
"""


def main():
    pass


if __name__ == '__main__':
    main()
