import pandas
import requests
import numpy
from check import detect_type
import time
from mattermostdriver import Driver
import ast

def send_message(message, channel_id='cwbtxf5m57frxg9f8974cqb1xy', host='159.223.232.203', MSA_token='atgdaqskupgbudg9i9izhtourh', Token='hst8z45bxp8cmjfu6a36pukkwe'):
    bot = Driver({
        'url': str(host),
        'token': Token,
        'scheme': 'http',
        'port': 8065,
        'basepath': '/api/v4',
        'verify': True,
        'mfa_token': MSA_token,
        'auth': None,
        'timeout': 30,
        'request_timeout': None,
        'keepalive': False,
        'keepalive_delay': 5,
        'websocket_kw_args': None,
        'debug': False,
    })
    bot.login()

    bot.posts.create_post(options={
        'channel_id': channel_id,
        'message': message,
    })

#translate = {'BANK':"Банк", 'TOKEN':"Токен", 'BIK':"БИК", 'INN':"ИНН", 'KPP':"КПП", 'CORESPONDENT_ACCOUNT':"Корр. счёт", 'MAIL': "Имейл", 'PHONE_NUMBER':"Телефон", 'NAME':"ФИО", 'BIRTHDAY_DAY':"День рождения", "QUESTION":"?", "RASCHET_ACCOUNT": "Расч. счет "}

class dlp:
    def __init__(self, filename):
        self.filename = filename
        if self.check():
            self.crm()
            self.fl = True
        else:
            self.fl = False

    def check(self):
        if self.filename[-4:] != ".csv":
            return False
        try:
            self.db = pandas.read_csv(self.filename, header=None, dtype="string")
        except:
            return False
        self.header = {}
        for i in self.db.columns:
            tmp = detect_type(str(self.db[i][0])).name
            if tmp in self.header:
                self.header[tmp].append(i)
            else:
                self.header[tmp] = [i]
        #print(list(self.header.keys()))
        return True
     

    def find_key(self, val):
        for i in self.header:
            if self.header[i][0] == int(val):
                return i
        return "QUESTION"

    def crm(self):
        mes = '{"action_type":"validate_danger_data", "data_list": ' + str(list(self.header.keys()))+"}"
        mes = mes.replace("\'", "\"")
        masks = requests.post('http://127.0.0.1:8000/api/danger_level_service/', json={"action_type":"validate_danger_data","data_list": list(self.header.keys())})
        masks  = (masks.content).decode()
        masks = ast.literal_eval(masks)
        if len(masks):
            send_message("Attention @all. User tried to download" + str(masks))
        for i in masks:
            for j in range(len(self.db[self.header[i][0]])):
                tmp = len(self.db[self.header[i][0]][j])
                self.db[self.header[i][0]][j] =  self.db[self.header[i][0]][j].strip()
                if tmp < 10:
                    self.db[self.header[i][0]][j] = '*'*tmp
                else:
                    self.db[self.header[i][0]][j] = self.db[self.header[i][0]][j][0] + '*'*(tmp-2) + self.db[self.header[i][0]][j][-1] 

        for i in self.header["QUESTION"]:
            for j in range(len(self.db[i])):
                tmp = len(self.db[i][j])
                self.db[i][j] =  self.db[i][j].strip()
                if tmp < 10:
                    self.db[i][j] = '*'*tmp
                else:
                    self.db[i][j] = self.db[i][j][0] + '*'*(tmp-2) + self.db[i][j][-1] 

    def ret(self):
        if self.fl:
            csv = ''
            for i in range(len(self.db.columns)):
                csv += "|" + self.find_key(i) + "|"
                for j in range(len(self.db[i])):
                    csv += str(self.db[i][j]) + "\."
            return csv
        return "Error"
        
test = dlp("/home/kali/Documents/user_data.csv")
print(test.ret())

# def str_to_csv(ans):
#     d = {}
#     kostil = 0
#     ans = ans.split('|')
#     for i in range(len(ans)):
#         if (i%2!=0 or i==0):
#             pass
#         else:
#             if ans[i-1] == "QUESTION":
#                 ans[i-1] = "QUESTION" + str(kostil)
#                 kostil+=1
#             d[ans[i-1]] = ans[i].split("\.")
#     db1 = pandas.DataFrame.from_dict(d)
#     db1.drop(db1.index[-1])
#     db1.to_csv("test.csv")

# str_to_csv(ans)