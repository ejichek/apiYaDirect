import json

with open('/home/bdataadmin/airflowYaDirect/skripts/json_YaDirect_constant.json', 'r', encoding="utf-8") as jsonFile:
    jsonData = json.load(jsonFile)
    
path_txt_input = jsonData["YaDirectApiConstant"]["path_intermediate"]
path_txt_output = jsonData["YaDirectApiConstant"]["path_final"]

with open(path_txt_input, 'r', encoding='utf-8') as f:
    data_file = f.read().splitlines(True)
    
if len(data_file)-4 == int(data_file[len(data_file) -1].replace("Total rows: ", "")):
    #data_zagolovok = data_file[2]
    all_clean_data = data_file[2:len(data_file)-2]
    with open(path_txt_output, 'w', encoding='utf-8') as f1:
        f1 = f1.writelines(all_clean_data)
    print("Успех! Чекай файл")
else:
    print("Что-то не так с размером файла")
    print("LEN: ", len(data_file))
    print("Total_rows: ", data_file[len(data_file) -1])