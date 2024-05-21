import concurrent
import csv
import sys
import time
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Thread, Lock
import threading
from tkinter import *
import tkinter as tk
# import tksheet
import pathlib

class SharableSpreadSheet:
    data = None

    def __init__(self,nRows, nCols):
        self.data = [["" for i in range(nCols)] for j in range (nRows)]
        self.nRows = nRows
        self.nCols = nCols
        self.write_rows =[threading.Semaphore()for i in range(nRows)]
        self.read_rows = [threading.Semaphore(1000) for i in range(nRows)]
        self.lock_cols = [threading.Semaphore() for i in range(nCols)]
        self.readCountRow = [0 for i in range(nRows)]
        self.readWriteCols = [0 for i in range(nCols)]
        self.IfLockCols = [0 for i in range(nCols)]
        self.ifswitch = 0



    def get_cell(self,row, cols):
        conditions = [row >= self.nRows, cols >= self.nCols, row < 0, cols < 0]
        get_check = self.conditionscheck(conditions)
        if get_check is False: return None

        while self.IfLockCols[cols] == 1:
            wait = 1

        self.read_rows[row].acquire()  # wait on read semaphore
        self.readCountRow[row] += 1  # increase count for reader by 1
        self.readWriteCols[cols] += 1
        self.write_rows[row].acquire()  # lock write semaphore
        cell = self.data[row][cols]
        self.readCountRow[row] -= 1    # reduce count for reader by 1
        self.readWriteCols[cols] -= 1
        if self.readCountRow[row] == 0:  # if no reader is present allow writer to write the data
            self.write_rows[row].release()
        self.read_rows[row].release()
        return cell

    def set_cell(self,row, cols, string):
        # return the first cell that contains the string [row,col]
        # return [-1,-1] if don't exists
        conditions = [row >= self.nRows, cols >= self.nCols, row < 0, cols < 0]
        get_check = self.conditionscheck(conditions)
        if get_check is False: return False

        while self.IfLockCols[cols] == 1:
            wait = 1
        self.write_rows[row].acquire()
        self.readWriteCols[cols] += 1
        self.data[row][cols] = string    # write the data
        self.write_rows[row].release()   # sinal on write semaphore
        self.readWriteCols[cols] -= 1


    def search_string(self,str_to_search):
        for i in range(self.nRows):
            for j in range(self.nCols):
                if self.get_cell(i,j) == str_to_search:
                    return [i,j]
        return [-1, -1]



    def exchange_rows(self,row1, row2):
        # exchange the content of row1 and row2
        conditions = [row1 >= self.nRows, row2 >= self.nRows, row1 < 0, row2 < 0]
        get_check = self.conditionscheck(conditions)
        if get_check is False: return False

        self.write_rows[row1].acquire()
        self.write_rows[row2].acquire()
        self.ifswitch = 1
        newrow1 = self.data[row1]
        newrow2 = self.data[row2]
        self.data[row1] = newrow2
        self.data[row2] = newrow1
        self.write_rows[row1].release()
        self.write_rows[row2].release()
        self.ifswitch = 0


    def exchange_cols(self,col1, col2):
        # exchange the content of col1 and col2
        conditions = [col1 >= self.nCols, col2 >= self.nCols, col1 < 0, col2 < 0]
        get_check = self.conditionscheck(conditions)
        if get_check is False: return False

        while self.ifswitch == 1 or self.readWriteCols[col1] > 0 or self.readWriteCols[col2] > 0:
            f =0
        self.lock_cols[col1].acquire()
        self.lock_cols[col2].acquire()
        self.readWriteCols[col1] += 1
        self.readWriteCols[col2] += 1
        self.IfLockCols[col1] = 1
        self.IfLockCols[col2] = 1
        for i in range(self.nRows):
            a = self.data[i][col1]
            self.data[i][col1] = self.data[i][col2]
            self.data[i][col2] = a
        self.lock_cols[col1].release()
        self.lock_cols[col2].release()
        self.readWriteCols[col1] -= 1
        self.readWriteCols[col2] -= 1
        self.IfLockCols[col1] = 0
        self.IfLockCols[col2] = 0


    def search_in_row(self,row_num, str_to_search):
        # perform search in specific row, return col number if exists.
        # return -1 otherwise
        conditions = [row_num >= self.nRows, row_num < 0]
        get_check = self.conditionscheck(conditions)
        if get_check is False: return False

        for i in range(self.nCols):
            if self.get_cell(row_num, i) == str_to_search:
                return i
        return -1

    def search_in_col(self,col_num, str_to_search):
        # perform search in specific col, return row number if exists.
        # return -1 otherwise
        conditions = [col_num >= self.nCols, col_num < 0]
        get_check = self.conditionscheck(conditions)
        if get_check is False: return False

        for i in range(self.nRows):
            if self.get_cell(i, col_num) == str_to_search:
                return i
        return -1


    def search_in_range(self,col1, col2, row1, row2, str_to_search):
        # perform search within spesific range: [row1:row2,col1:col2]
        # includes col1,col2,row1,row2
        # return the first cell that contains the string [row,col]
        # return [-1,-1] if don't exists
        conditions = [col1 >= self.nCols, col2 >= self.nCols, row1 >= self.nRows, row2 >= self.nRows, row1 < 0,
                      row2 < 0, col1 < 0, col2 < 0]
        get_check = self.conditionscheck(conditions)
        if get_check is False: return False

        for i in range(row1, row2+1):
            for j in range(col1,col2+1):
                if self.get_cell(i,j) == str_to_search:
                    return [i,j]

        return [-1,-1]


    def add_row(self,row1):
        # add a row after row1
        conditions = [row1 >= self.nRows, row1 < 0]
        get_check = self.conditionscheck(conditions)
        if get_check is False: return False

        self.data.append(["" for i in range(self.nCols)])
        self.nRows += 1

        self.write_rows.append(threading.Semaphore())
        self.read_rows.append(threading.Semaphore())
        self.readCountRow.append(0)

        for i in range(self.nRows -2, row1, -1): # *******
            self.exchange_rows(i, i+1)



    def add_col(self,col1):
        # add a col after col1
        conditions = [col1 >= self.nCols, col1 < 0]
        get_check = self.conditionscheck(conditions)
        if get_check is False: return False

        for i in self.data:
            i.append("")
        self.nCols += 1
        self.lock_cols.append(threading.Semaphore())
        self.readWriteCols.append(0)
        self.IfLockCols.append(0)
        # self.IfLockCols.append(0)

        for i in range(self.nCols - 2, col1, -1):
            self.exchange_cols(i, i + 1)


    def save(self,f_name):
        # save the spreadsheet to a file fileName as following:
        # nRows,nCols
        # row,col, string
        # row,col, string
        # row,col, string
        # For example 50X50 spread sheet size with only 3 cells with strings:
        # 50,50
        # 3,4,"Hi"
        # 5,10,"OOO"
        # 13,2,"EE"
        # you can decide the saved file extension.
        with open(f_name, "w") as file:
            file.writelines("{},{}\n".format(self.nRows, self.nCols))
            for i in range(self.nRows):
                for j in range(self.nCols):
                    if self.data[i][j] != "":
                        file.writelines('{},{},{}\n'.format(i,j,self.data[i][j]))

        return True


    def conditionscheck(self, conditions):
        if any(conditions):
            return False
        else:
            return True

    def load(self,f_name):
        # # load the spreadsheet from fileName
        # # replace the data and size of the current spreadsheet with the loaded data

        file = pathlib.Path(f_name)
        if not file.exists():
            return False

        file1 = open(f_name , 'r')
        Lines = file1.readlines()
        line1 = Lines[0].split(",")
        row = line1[0]
        cols = line1[1]
        self.data =[["" for i in range(int(cols))] for j in range(int(row))]
        self.nRows = int(row)
        self.nCols = int(cols)
        self.write_rows = [threading.Semaphore() for i in range(int(row))]
        self.read_rows = [threading.Semaphore(1000) for i in range(int(row))]
        self.lock_cols = [threading.Semaphore() for i in range(int(cols))]
        self.readCountRow = [0 for i in range(int(row))]
        self.readWriteCols = [0 for i in range(int(cols))]
        self.IfLockCols = [0 for i in range(int(cols))]
        self.ifswitch = 0
        for i in Lines[1:]:
            line = i.split(",")
            self.data[int(line[0])][int(line[1])]=line[2].replace("\n", "")
        return True



    def show(self):
        # show the spreadsheet using tkinker.
        # tkinker is the default python GUI library.
        # as part of the HW you should learn how to use it.
        # there are links and simple example in the last practical lesson on model
        lst = self.data
        root = Tk()

        total_rows = len(lst)
        total_columns = len(lst[0])
        if total_rows < 30 and  total_columns < 30:
            class Table:

                def __init__(self, root):

                    # code for creating table
                    for i in range(total_rows):
                        for j in range(total_columns):
                            self.e = Entry(root, background="pink", width=7, fg='black',bg="pink",borderwidth=3,
                                           font=('David', 10, 'bold'), justify=tk.CENTER)

                            self.e.grid(row=i, column=j , padx=1 , pady= 1)
                            self.e.insert(END, lst[i][j])
        else:
            class Table:

                def __init__(self, root):

                    # code for creating table
                    for i in range(total_rows):
                        for j in range(total_columns ):
                            self.e = Entry(root, background="pink", width=5, fg='black', bg="pink", borderwidth=3,
                                           font=('David', 6, 'bold'), justify=tk.CENTER)

                            self.e.grid(row=i, column=j, padx=1, pady=1)
                            self.e.insert(END, lst[i][j])


        t = Table(root)
        root.mainloop()
        return True


# # take the data
import random

def spread_sheet_tester(nUsers, nTasks, spreadsheet):
    sheet = spreadsheet
    str_lst = ["nitz", "eden", "amit", "yonatan", "nir", "maya", "merav", "omer", "barak",
               "barko", "baras", "baka", "fink"]
    row = spreadsheet.nRows
    col = spreadsheet.nCols
    with concurrent.futures.ThreadPoolExecutor(nUsers) as executor:
        for i in range(nTasks):
            task = random.randint(1, 10)

            num_row, num_row2 = sorted(random.sample(range(row), 2))
            num_col, num_col2 = sorted(random.sample(range(col), 2))
            my_str = random.sample(str_lst, 1)[0]

            # shitty syntax dont look
            if task == 1:
                executor.submit(sheet.get_cell, num_row, num_col)
            elif task == 2:
                executor.submit(sheet.set_cell, num_row, num_col, my_str)
            elif task == 3:
                executor.submit(sheet.search_string, my_str)
            elif task == 4:
                executor.submit(sheet.exchange_rows, num_row, num_row2)
            elif task == 5:
                executor.submit(sheet.exchange_cols, num_col2, num_col)
            elif task == 6:
                executor.submit(sheet.search_in_row, num_row, my_str)
            elif task == 7:
                executor.submit(sheet.search_in_col, num_col, my_str)
            elif task == 8:
                executor.submit(sheet.search_in_range, num_row, num_row2, num_col, num_col2, my_str)
            elif task == 9:
                executor.submit(sheet.add_row, num_row)
            else:
                executor.submit(sheet.add_col, num_col)

    return sheet


def external_test(n_rows, n_cols, n_users, n_tasks):
    test_spread_sheet = SharableSpreadSheet(n_rows, n_cols)
    test_spread_sheet = spread_sheet_tester(n_users, n_tasks,test_spread_sheet)
    test_spread_sheet.show()
    test_spread_sheet.save('external_test_saved.dat')


if __name__ == '__main__':
    if len(sys.argv)==5:
        external_test(n_rows=sys.argv[1],n_cols=sys.argv[2],n_users=sys.argv[3],n_tasks=sys.argv[4])
    else:
        #Internal test example (you can change it to check yourself)
        #create, test and save SharableSpreadSheet
        ss = SharableSpreadSheet(5, 5)
        ss = spread_sheet_tester(10, 10, ss)
        ss.show()
        ss.save('saved.dat')
        load_ss = SharableSpreadSheet(3, 3)
        load_ss.load('saved.dat')
        load_ss = spread_sheet_tester(4, 4, load_ss)
        load_ss.show()


