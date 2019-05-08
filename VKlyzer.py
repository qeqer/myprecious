import sys
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *
import pandas as pd
import webbrowser
import json
import re
from tools import VkParser, PredictorV2

"""id в файле должны быть через запятую"""


class VkParserDec(VkParser):
    def __init__(self, login, password):
        super().__init__(login=login, password=password)

    def predict_all(self, addresses, wall_collect):
        self.wall_collect = int(wall_collect)
        ex_a = self.collect_easy_features(self.format_addresses(addresses))
        ex_b = self.extract_easy_features(ex_a)
        ex_c = self.extract_final(ex_b)
        ex_d = self.normalize(ex_c)
        ex_r = self.predict(ex_d)
        res = pd.concat((df for df in ex_r), axis=0, ignore_index=True)
        dt_te = pd.DataFrame(res.res.values.tolist(), index=res.index)
        res[["score", "concl"]] = dt_te if len(dt_te > 0) else\
            pd.DataFrame(columns=["score", "concl"])
        res = res.drop(["res"], axis=1)
        res["concl"] = res["concl"].apply(lambda x: "Бот" if x is True else "Норм")
        res.columns = ["Адрес", "Состояние", "Оценка", "Итог"]
        print("Completed\n")
        return res, ex_c


class Login(QDialog):
    def __init__(self, parent=None):
        super(Login, self).__init__(parent)
        self.setWindowTitle("Login")
        self.textName = QLineEdit(self)
        self.textName.setPlaceholderText("Tel")
        self.textPass = QLineEdit(self)
        self.textPass.setPlaceholderText("Password")
        self.button_log = QPushButton("Login", self)
        self.button_log.clicked.connect(self.cls)
        layout = QVBoxLayout(self)
        layout.addWidget(self.textName)
        layout.addWidget(self.textPass)
        layout.addWidget(self.button_log)

    def cls(self):
        self.accept()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.log_file_name = "log.csv"
        self.res_file_name = "res.nsv"

    def ask_pass(self):
        try:
            with open("passfile.json") as in_f:
                self.login, self.password = json.load(in_f)
            self.parser = VkParserDec(self.login, self.password)
            if (self.parser.ErrorCode != 0):
                q = QMessageBox()
                q.setText("Wrong log/pass in file")
                raise OSError
            else:
                self.analyze_button.setEnabled(True)

        except OSError:
            dial = Login()
            params = dial.exec_()
            if (params == QDialog.Accepted):
                self.login = dial.textName.text()
                self.password = dial.textPass.text()
                self.parser = VkParserDec(self.login, self.password)
                if (self.parser.ErrorCode != 0):
                    q = QMessageBox()
                    q.setText("Wrong log/pass")
                else:
                    self.analyze_button.setEnabled(True)
            dial.setAttribute(Qt.WA_DeleteOnClose)

    def change_source(self):
        if (self.from_file_check.isChecked() is True):
            self.grid.removeWidget(self.page_ask_field)
            self.page_ask_field.setParent(None)
            self.grid.addWidget(self.file_ask_button, 4, 0, 1, 2)

        else:
            self.grid.removeWidget(self.file_ask_button)
            self.file_ask_button.setParent(None)
            self.grid.addWidget(self.page_ask_field, 4, 0, 1, 2)

    def get_file_name(self):
        f_name = QFileDialog.getOpenFileName(self, "Open File", ".")
        if (f_name != ""):
            self.file_name = f_name[0]

    def analyze(self):
        page_ids = []
        # читаем из файла id
        if (self.from_file_check.isChecked() is True):
            with open(self.file_name) as in_file:
                page_ids = in_file.readline().rstrip().split(",")
        else:
            txte = self.page_ask_field.text()
            if (txte != ""):
                txte = re.split(r" |, |,", txte)
                page_ids = [i for i in txte if i != ""]
            else:
                page_ids = []

        # возвращает pandas dataframe и pandas dataframe
        if (len(page_ids) < 1):
            self.info.insertPlainText("EMPTY!")
            return
        res, logs = self.parser.predict_all(page_ids, self.wall_check.isChecked())

        self.info.clear()
        if (res.empty):
            self.info.insertPlainText("EMPTY!")
            return
        if (self.to_file.isChecked() is False):
            if (self.log_check.isChecked() is True):
                with open(self.log_file_name, "w") as out_file:
                    print(logs.to_string(), file=out_file)
            self.info.insertPlainText("Result: \n" + res.to_string())
        else:
            if (self.log_check.isChecked() is True):
                with open(self.log_file_name, "w") as out_file:
                    print(logs.to_string(), file=out_file)
            with open(self.res_file_name, "w", encoding="utf8") as out_file:
                print(res.to_string(), file=out_file)
            self.info.insertPlainText("Finished!")

    def open_url(self):
        url = self.page_ask_field.text()
        if ("vk.com" not in url and "https//" not in url):
            url = "https://vk.com/" + url
        elif ("https//" not in url):
            url = "https://" + url
        webbrowser.open(url)

    def initUI(self):
        self.wid = QWidget(self)
        self.setCentralWidget(self.wid)

        self.grid = QGridLayout()
        # self.grid.setColumnStretch(1, 1)
        self.grid.setColumnMinimumWidth(1, 50)
        self.grid.setColumnMinimumWidth(0, 50)
        self.grid.setColumnStretch(3, 1)
        self.grid.setSpacing(10)

        self.log_button = QPushButton("Log IN")
        self.log_button.clicked.connect(self.ask_pass)
        self.grid.addWidget(self.log_button, 1, 0, 1, 2)

        self.from_file_check = QCheckBox("From File")
        self.from_file_check.setToolTip("Get IDs from file. ids must be in csv format")
        self.from_file_check.setChecked(False)
        self.from_file_check.stateChanged.connect(self.change_source)
        self.grid.addWidget(self.from_file_check, 3, 0)

        self.log_check = QCheckBox("Extra Info")
        self.log_check.setToolTip("Output extra info about account in log file")
        self.log_check.setChecked(False)
        self.grid.addWidget(self.log_check, 3, 1)

        self.wall_check = QCheckBox("Wall Collect")
        self.wall_check.setToolTip("Collect wall. Slows down process + limit on wall querries")
        self.wall_check.setChecked(False)
        self.grid.addWidget(self.wall_check, 2, 0)

        self.to_file = QCheckBox("Res to file")
        self.to_file.setToolTip("Output results to res file instead of window")
        self.to_file.setChecked(False)
        self.grid.addWidget(self.to_file, 2, 1)

        self.file_ask_button = QPushButton("Load IDs")
        self.file_ask_button.setToolTip("Load IDs from file")
        self.file_ask_button.clicked.connect(self.get_file_name)
        self.file_name = ""

        self.page_ask_field = QLineEdit()
        self.page_ask_field.setText("0x333")
        self.grid.addWidget(self.page_ask_field, 4, 0, 1, 2)

        self.analyze_button = QPushButton("Analyze")
        self.analyze_button.clicked.connect(self.analyze)
        self.analyze_button.setEnabled(False)
        self.grid.addWidget(self.analyze_button, 5, 0, 1, 2)

        self.open_button = QPushButton("Open Page")
        self.open_button.clicked.connect(self.open_url)
        self.grid.addWidget(self.open_button, 6, 0, 1, 2)

        self.info = QPlainTextEdit()
        self.info.setFont(QFont("Lucida Console", 10))
        self.grid.addWidget(self.info, 0, 3, 7, 1)

        self.wid.setLayout(self.grid)

        self.setGeometry(300, 300, 600, 400)
        self.setWindowTitle('VKlyzer')
        # self.setAutoFillBackground(True)

        self.show()


def CreateMainWindow():
    app = QApplication(sys.argv)
    ex = MainWindow()
    sys.exit(app.exec_())


def main():
    CreateMainWindow()


if __name__ == '__main__':
    main()
