# -*- coding: utf-8 -*-
import requests
from requests.exceptions import ConnectionError
from time import sleep
import json
import sys
import os


def getYaDirect_custum1(inputDate):
    with open('/home/bdataadmin/airflowYaDirect/skripts/json_YaDirect_constant.json', 'r', encoding="utf-8") as jsonFile:
        jsonData = json.load(jsonFile)

    date = str(inputDate)
    #    date = "2024-11-05"
    pathLogsYaDirect = jsonData["YaDirectApiConstant"]["path_logs"]
    path_intermediate_txt = jsonData["YaDirectApiConstant"]["path_intermediate"]
    proxy = {'https': f'{jsonData["YaDirectApiConstant"]["proxy"]}'}

    if os.path.exists(pathLogsYaDirect) == True: 
        os.remove(pathLogsYaDirect)
        
    # Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
    if sys.version_info < (3,):
        def u(x):
            try:
                return x.encode("utf8")
            except UnicodeDecodeError:
                return x
    else:
        def u(x):
            if type(x) == type(b''):
                return x.decode('utf8')
            else:
                return x
        
    # --- Входные данные ---
    # Адрес сервиса Reports для отправки JSON-запросов (регистрозависимый)
    ReportsURL = 'https://api.direct.yandex.com/json/v5/reports'
        
    # OAuth-токен пользователя, от имени которого будут выполняться запросы
    token = 'y0_AgAAAAByMQIbAAr4ZwAAAAD0eZTuiy7tfWMnT1WzMHoVJqV30DoiK8g'
        
    # Логин клиента рекламного агентства
    # Обязательный параметр, если запросы выполняются от имени рекламного агентства
    #clientLogin = 'ЛОГИН_КЛИЕНТА'
        
    # --- Подготовка запроса ---
    # Создание HTTP-заголовков запроса
    headers = {
                # OAuth-токен. Использование слова Bearer обязательно
                "Authorization": "Bearer " + token,
                # Логин клиента рекламного агентства
        #       "Client-Login": clientLogin,
                # Язык ответных сообщений
                "Accept-Language": "ru",
                # Режим формирования отчета
                "processingMode": "auto"
               # Формат денежных значений в отчете
               # "returnMoneyInMicros": "false",
               # Не выводить в отчете строку с названием отчета и диапазоном дат
               # "skipReportHeader": "true",
               # Не выводить в отчете строку с названиями полей
               # "skipColumnHeader": "true",
               # Не выводить в отчете строку с количеством строк статистики
               # "skipReportSummary": "true"
               }

    # Создание тела запроса
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date,
                "DateTo": date
            },
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",
                "CampaignType",
                "AdGroupId",
                "AdGroupName",
                "AdId",
                "AdFormat",
                "AdNetworkType",
                "CarrierType",
                "Device",
                "MobilePlatform",
                "Gender",
                "Age",
                "IncomeGrade",
                "TargetingLocationName",
                "LocationOfPresenceName",
                "Criterion",
                "CriterionType",
                "Placement",
                "TargetingCategory",
                "Slot",
                "Impressions",
                "Clicks",
                "Cost",
                "Sessions",
                "Bounces",
                "AvgClickPosition",
                "AvgEffectiveBid",
                "AvgImpressionPosition",
                "AvgPageviews",
                "AvgTrafficVolume",
                "Revenue",
                "Conversions"
            ],
            "ReportName": u("AFLT_BigDataReport_"+date),
            "ReportType": "CUSTOM_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO"
        }
    }

    # Кодирование тела запроса в JSON
    body = json.dumps(body, indent=4)

    # --- Запуск цикла для выполнения запросов ---
    # Если получен HTTP-код 200, то выводится содержание отчета
    # Если получен HTTP-код 201 или 202, выполняются повторные запросы
    while True:
        try:
            req = requests.post(ReportsURL, body, headers=headers, proxies=proxy)
            req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
            if req.status_code == 400:
                print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(u(body)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                txt = "\nstatus_code: 400\n"+"Параметры запроса указаны неверно или достигнут лимит отчетов в очереди\n"+"RequestId: {}".format(req.headers.get("RequestId", False))+"\n"+"JSON-код запроса: {}".format(u(body))+"\n"+"JSON-код ответа сервера: \n{}".format(u(req.json()))+"\n"
                with open(pathLogsYaDirect, 'a', encoding='utf-8') as f:
                    f = f.write(txt)
                break
            elif req.status_code == 200:
                print("Отчет создан успешно")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                #print("Содержание отчета: \n{}".format(u(req.text)))
                with open(path_intermediate_txt, 'w', encoding='utf-8') as f:
                    f.write("Содержание отчета: \n{}".format(u(req.text)))
                print("Успехшно записано, чекай файл")
                txt = "\nstatus_code: 200\n"+"Файл успешно записан - чекай файл"
                with open(pathLogsYaDirect, 'a', encoding='utf-8') as f:
                    f = f.write(txt)
                break
            elif req.status_code == 201:
                print("Отчет успешно поставлен в очередь в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                txt = "\nstatus_code: 201\n"+"Отчет успешно поставлен в очередь в режиме офлайн\n"+"Повторная отправка запроса через {} секунд".format(retryIn)+"\n"+"RequestId: {}".format(req.headers.get("RequestId", False))
                with open(pathLogsYaDirect, 'a', encoding='utf-8') as f:
                    f = f.write(txt)
                sleep(retryIn)
            elif req.status_code == 202:
                print("Отчет формируется в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                txt = "\nstatus_code: 202\n"+"Повторная отправка запроса через {} секунд".format(retryIn)+"\n"+"RequestId:  {}".format(req.headers.get("RequestId", False))+"\n" 
                with open(pathLogsYaDirect, 'a', encoding='utf-8') as f:
                    f = f.write(txt)
                sleep(retryIn)
            elif req.status_code == 500:
                print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                txt = "\nstatus_code: 500\n"+"RequestId: {}".format(req.headers.get("RequestId", False))+"JSON-код ответа сервера: \n{}".format(u(req.json()))+"\n"
                with open(pathLogsYaDirect, 'a', encoding='utf-8') as f:
                    f = f.write(txt)                
                break
            elif req.status_code == 502:
                print("Время формирования отчета превысило серверное ограничение.")
                print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                print("JSON-код запроса: {}".format(body))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                txt = "\nstatus_code: 502\n"+"Время формирования отчета превысило серверное ограничение.\n"+ "JSON-код запроса: {}".format(body)+"\n"+"RequestId: {}".format(req.headers.get("RequestId", False))+"\n"+"JSON-код ответа сервера: \n{}".format(u(req.json()))+"\n"  
                with open(pathLogsYaDirect, 'a', encoding='utf-8') as f:
                    f = f.write(txt)   
                break
            else:
                print("Произошла непредвиденная ошибка")
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(body))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                txt = "\nПроизошла непредвиденная ошибка без status_code\n"+"RequestId:  {}".format(req.headers.get("RequestId", False))+"\n"+"JSON-код запроса: {}".format(body)+"\n"+"JSON-код ответа сервера: \n{}".format(u(req.json()))+"\n"
                with open(pathLogsYaDirect, 'a', encoding='utf-8') as f:
                    f = f.write(txt)
                break

        # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except ConnectionError:
            # В данном случае мы рекомендуем повторить запрос позднее
            print("Произошла ошибка соединения с сервером API")
            txt = "ConnectionError: Произошла ошибка соединения с сервером API"
            with open(pathLogsYaDirect, 'a', encoding='utf-8') as f:
                f = f.write(txt)
            # Принудительный выход из цикла
            break

        # Если возникла какая-либо другая ошибка
        except:
            # В данном случае мы рекомендуем проанализировать действия приложения
            print("Произошла непредвиденная ошибка")
            txt = "Произошла непредвиденная ошибка"
            with open(pathLogsYaDirect, 'a', encoding='utf-8') as f:
                f = f.write(txt)
            # Принудительный выход из цикла
            break
            
if __name__ == '__main__':
    getYaDirect_custum1(sys.argv[1])