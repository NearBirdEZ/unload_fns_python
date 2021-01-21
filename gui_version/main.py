#!/usr/bin/env python3

import threading
import tkinter as tk
from tkinter import ttk
from tkcalendar import Calendar
from tkinter import scrolledtext
from tkinter import messagebox
from datetime import datetime
from fns import *
import re


class App:

    def __init__(self, gui_window):
        self.gui_window = gui_window
        self.gui_window.title('Выгрузка ИФНС')
        # self.gui_window.iconbitmap(os.getcwd() + '\\icon.ico')
        self.gui_window.geometry('600x600')
        self.gui_window['bg'] = '#5F5F5F'

        """Блок номера заявки и количества потоков"""
        """----------------------------------------------------------------------------------------------------------"""
        tk.Label(self.gui_window, text='Номер заявки', font=("Arial Bold", 14), bg='#5F5F5F',
                 fg='white').place(relx=0.14, rely=0.01)
        tk.Label(self.gui_window, text='Количество потоков', font=("Arial Bold", 14), bg='#5F5F5F', fg='white').place(
            relx=0.58, rely=0.01)
        self.request = tk.Entry(self.gui_window, width=30)
        self.request.place(relx=0.05, rely=0.07)
        self.request.bind('<KeyPress>', lambda x: "break" if x.keysym not in (
                list("1234567890") + ['BackSpace', 'Delete', 'Left', 'Right']) else "")
        self.threads = tk.Entry(self.gui_window, width=30)
        self.threads.place(relx=0.55, rely=0.07)
        self.threads.insert(tk.END, '10')
        self.threads.bind('<KeyPress>', lambda x: "break" if x.keysym not in (
                list("1234567890") + ['BackSpace', 'Delete', 'Left', 'Right']) else "")

        """Блок даты"""
        """----------------------------------------------------------------------------------------------------------"""

        def get_date(flag: int, lbl):
            def print_sel():
                lbl['text'] = cal.selection_get()
                lbl['borderwidth'] = 2
                lbl['relief'] = 'solid'
                self.date_list[flag] = datetime.combine(cal.selection_get(), dt.datetime.min.time())
                top.destroy()

            top = tk.Toplevel(gui_window)

            cal = Calendar(top,
                           font="Arial 14", selectmode='day',
                           cursor="hand1", year=2020, month=1, day=1)
            cal.pack(fill="both", expand=True)
            tk.Button(top, text="ok", command=print_sel).pack()

        self.date_list = [0, 0]

        lbl_from_date = tk.Label(self.gui_window, text='', font=("Arial Bold", 14), bg='#5F5F5F', fg='white')
        lbl_from_date.place(relx=0.169, rely=0.215)

        lbl_to_date = tk.Label(self.gui_window, text='', font=("Arial Bold", 14), bg='#5F5F5F', fg='white')
        lbl_to_date.place(relx=0.655, rely=0.215)

        tk.Button(gui_window, text='From Date', font=("Arial Bold", 14),
                  command=lambda: get_date(0, lbl_from_date)).place(relx=0.16, rely=0.14)
        tk.Button(gui_window, text='To Date', font=("Arial Bold", 14),
                  command=lambda: get_date(1, lbl_to_date)).place(relx=0.665, rely=0.14)

        """Блок ИНН и РНМ"""
        """----------------------------------------------------------------------------------------------------------"""
        tk.Label(self.gui_window, text='ИНН компаний', font=("Arial Bold", 14), bg='#5F5F5F', fg='white').place(
            relx=0.145, rely=0.268)
        tk.Label(self.gui_window, text='РНМ', font=("Arial Bold", 14), bg='#5F5F5F', fg='white').place(
            relx=0.71, rely=0.268)

        self.inn_list = scrolledtext.ScrolledText(gui_window, width=22, height=17, font=("Arial Bold", 12))
        self.rnm_list = scrolledtext.ScrolledText(gui_window, width=22, height=17, font=("Arial Bold", 12))
        self.inn_list.place(relx=0.07, rely=0.33)
        self.rnm_list.place(relx=0.57, rely=0.33)

        """Блок старта"""
        """----------------------------------------------------------------------------------------------------------"""
        self.btn_start = tk.Button(gui_window, text='Запустить выгрузку', command=self.begin, bg='#5F5F5F',
                                   fg='white', font=("Arial", 13))
        self.btn_start.place(relx=0.355, rely=0.9)

    def func(self):

        request = self.request.get()
        threads = int(self.threads.get())
        inn_list = re.findall(r'\d{10,12}', self.inn_list.get('0.1', tk.END))
        rnm_list = re.findall(r'\d{16}', self.rnm_list.get('0.1', tk.END))

        inn_string = ', '.join(f"'{inn}'" for inn in inn_list)

        if len(rnm_list) != 0:
            rnm_string = ', '.join(f"'{rnm}'" for rnm in rnm_list)
            rnm_string = f'and kkt.register_number_kkt in ({rnm_string})'
        else:
            rnm_string = ''

        self.btn_start.destroy()

        if get_version():
            uf = UnloadFns(request, threads, self.date_list, inn_string, rnm_string, self.gui_window)

            lbl_progress_bar = tk.Label(self.gui_window, text='Получение пар РНМ:ФН',
                                        font=("Arial Bold", 14), bg='#5F5F5F', fg='white')
            lbl_progress_bar.place(relx=0.355, rely=0.9)

            uf.bar = ttk.Progressbar(self.gui_window, mode='determinate', maximum=0, length=584)
            uf.bar.place(relx=0.01, rely=0.95)
            dict_inn_rnm_fn = uf.get_dict_inn_rnm_fn()
            unload_flag = uf.analysis()
            if unload_flag:
                max_bar = uf.count_fn * 2 * uf.threads
            else:
                max_bar = uf.count_fn * 2
            lbl_progress_bar['text'] = 'Общий прогресс'
            uf.bar.destroy()
            uf.bar = ttk.Progressbar(self.gui_window, mode='determinate', maximum=max_bar, length=584)
            uf.bar.place(relx=0.01, rely=0.95)
            for inn, rnm_fn_list in dict_inn_rnm_fn.items():
                if len(rnm_fn_list) != 0:
                    uf.start_threading(inn, rnm_fn_list, unload_flag)

            lbl_progress_bar['text'] = 'Архивация'
            uf.bar.destroy()
            uf.bar = ttk.Progressbar(self.gui_window, mode='determinate', maximum=0, length=584)
            uf.bar.place(relx=0.01, rely=0.95)
            uf.count_bar = 0
            uf.final_zip()
            messagebox.showinfo("Готово")
            #notify(uf.request)
        else:
            tk.Label(self.gui_window,
                     text='Вышла новая версия скрипта. Обновись:\nhttps://github.com/NearBirdEZ/unload_fns_python',
                     font=("Arial Bold", 14), bg='#5F5F5F', fg='white').place(relx=0.01, rely=0.9)

            print('Вышла новая версия скрипта. Обновись: https://github.com/NearBirdEZ/unload_fns_python')

    def begin(self):
        if self.request.get().strip() \
                and self.threads.get().strip() \
                and 0 not in self.date_list \
                and self.inn_list.get('0.1', tk.END).strip():
            threading.Thread(target=self.func, daemon=True).start()
        else:
            messagebox.showinfo('Ошибка', 'Не заполнены поля')


if __name__ == '__main__':
    gui_window = tk.Tk()
    app = App(gui_window)
    gui_window.mainloop()
