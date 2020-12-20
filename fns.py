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
from progress.bar import ChargingBar
import notify2


def get_version():
    v = requests.get('https://github.com/NearBirdEZ/unload_fns_python/blob/main/properties')
    soup = BeautifulSoup(v.text, 'html.parser')
    version_online = soup.find('td', class_='blob-code blob-code-inner js-file-line').get_text()
    with open('properties', 'r') as vars:
        for line in vars:
            if line.strip().startswith('version'):
                local_version = line.strip()
                break
    return local_version == version_online


class UnloadFns:

    def __init__(self, inner_vars_file):
        inner_vars = self.__open_request(inner_vars_file)
        self.request = inner_vars[0]
        self.threads = inner_vars[1]
        self.date_list = self.__division_by_month(inner_vars[2])
        self.inn_string = inner_vars[3]
        self.rnm_string = inner_vars[4]
        self.connect = Connections('properties')
        self.__job_folder()
        self.global_count_bar = 0
        self.bar = None
        self.lock = Lock()

    def __open_request(self, file):
        """Считываем входные данные заявки"""
        inn_list = []
        rnm_list = []
        date_in = []
        count = 0
        with open(file, 'r') as vars:
            for line in vars:
                if line.strip().startswith('request-number'):
                    request_number = line.strip().split('=')[1]
                elif line.strip().startswith('threads'):
                    threads = int(line.strip().split('=')[1])
                elif line.strip().startswith('from-Date'):
                    date_in.append(line.strip().split('=')[1])
                elif line.strip().startswith('to-Date'):
                    date_in.append(line.strip().split('=')[1])
                elif line.strip().startswith('ИНН'):
                    count = 1
                elif line.strip().startswith('Регистрационный'):
                    count = 2
                elif count == 1 and line.strip() != '':
                    inn_list.append(line.strip())
                elif count == 2 and line.strip() != '':
                    rnm_list.append(line.strip())

            inn_string = ', '.join(f"'{inn}'" for inn in inn_list)

            if len(rnm_list) != 0:
                rnm_string = ', '.join(f"'{rnm}'" for rnm in rnm_list)
                rnm_string = f'and kkt.register_number_kkt in ({rnm_string})'
            else:
                rnm_string = ''

        return request_number, threads, date_in, inn_string, rnm_string

    def __division_by_month(self, date_in):
        # Разбиваем даты помесячно
        time1 = dt.datetime.fromisoformat(date_in[0])
        time2 = dt.datetime.fromisoformat(date_in[1]) + dt.timedelta(hours=23, minutes=59, seconds=59)
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
        if not os.path.exists(f"../unload_for_fns/"):
            os.mkdir(f"../unload_for_fns/")

        if not os.path.exists(f"../unload_for_fns/{self.request}/"):
            os.mkdir(f"../unload_for_fns/{self.request}/")
        os.chdir(f"../unload_for_fns/{self.request}/")

    def print_date(self):
        print(f'\nБыло получено {len(self.date_list)} период(ов)')
        for dates in self.date_list:
            print(f'С {dates[0]} по {dates[1]}')
        print()

    def init_bar(self, name_bar, max_bar):
        # Инициализация прогресс бара
        print()
        progress_bar = ChargingBar(name_bar, max=max_bar)
        return progress_bar

    def collect_rnm_inn(self):
        print('Запрос в базу данных...')
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
        count = 0
        rnm_inn_list = self.collect_rnm_inn()
        bar = self.init_bar('Получение пар РНМ:ФН', len(rnm_inn_list) * 2)
        three_inn_rnm_fn_dict = {}
        for rnm, inn in rnm_inn_list:
            bar.next()
            for fn in self.collect_fn(rnm):
                fn = fn['key']
                if three_inn_rnm_fn_dict.get(inn):
                    three_inn_rnm_fn_dict[inn].append((rnm, fn))
                else:
                    three_inn_rnm_fn_dict[inn] = [(rnm, fn)]
                count += 1
            bar.next()
        return three_inn_rnm_fn_dict, count

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
        index_list = ['receipt.*', 'open_shift', 'close_shift', 'fiscal_report', 'fiscal_report_correction',
                      'bso', 'bso_correction', 'current_state_report', 'close_archive']
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
        self.bar.next()
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
        with zipfile.ZipFile(f'{self.request}.zip', mode='w', compression=zipfile.ZIP_DEFLATED) as zipFile:
            # Обход всего дерева директории и сжатие файлов в каждой папке
            files = [file for file in os.listdir('.') if not file.endswith('.zip')]
            for folder in files:
                for root, dirs, files in os.walk(folder):  # Список всех файлов и папок в директории folder
                    for file in files:
                        file_name = os.path.join(root, file)
                        zipFile.write(file_name)  # Создание относительных путей и запись файлов в архив
                        os.remove(file_name)
                    if not os.listdir(root):
                        os.rmdir(root)
                os.rmdir(folder)

    def analysis(self):
        print(f'\n\nПроизводится настройка количества потоков относительно входных данных.'
              f'\nНа вход поступил запрос по использованию {self.threads} потоков')
        old_threads = self.threads
        count_month = len(self.date_list)
        count_fn = self.global_count_bar
        flag = None

        if self.threads >= max(count_month, count_fn):
            if count_month >= count_fn:
                self.threads = count_month
                flag = True
            else:
                self.threads = count_fn
                flag = False
        elif self.threads < max(count_month, count_fn):
            if count_month >= count_fn:
                self.threads = ceil(count_month / ceil(count_month / self.threads))
                flag = True
            else:
                self.threads = ceil(count_fn / ceil(count_fn / self.threads))
                flag = False
        if old_threads != self.threads:
            print(f'Количество потоков было изменено на {self.threads} для корректной загрузки')
        print('Выгрузка будет происходить каждый поток', *['свой месяц' if flag else 'свою ФН'])
        return flag


def timer_(func):
    def wrapper():
        start = dt.datetime.now()
        print(f'Время начала [{start}]')
        func()
        end = dt.datetime.now()
        print(f'\nВремя выполнения [{end - start}]')

    return wrapper


def notify(request):
    # Оповещалка об окончании выгрузки
    notify2.init(f'Выгрузка по заявке {request}')
    n = notify2.Notification(f'Выгрузка по заявке № {request} завершилась.')
    n.set_urgency(notify2.URGENCY_NORMAL)
    n.show()


@timer_
def main():
    if get_version():
        uf = UnloadFns('request.txt')
        uf.print_date()
        dict_inn_rnm_fn, uf.global_count_bar = uf.get_dict_inn_rnm_fn()
        unload_flag = uf.analysis()
        if unload_flag:
            max_bar = uf.global_count_bar * 2 * uf.threads
        else:
            max_bar = uf.global_count_bar * 2
        uf.bar = uf.init_bar('Общий прогресс выполнения', max_bar)
        for inn, rnm_fn_list in dict_inn_rnm_fn.items():
            if len(rnm_fn_list) != 0:
                uf.start_threading(inn, rnm_fn_list, unload_flag)
        print()
        uf.final_zip()
        notify(uf.request)
    else:
        print('Вышла новая версия скрипта. Обновись: https://github.com/NearBirdEZ/unload_fns_python')
    return


if __name__ == '__main__':
    main()
