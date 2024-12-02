import os
import json

with open('/home/bdataadmin/airflowYaDirect/skripts/json_YaDirect_constant.json', 'r', encoding="utf-8") as jsonFile:
    jsonData = json.load(jsonFile)

def clear_logs_folder(path_intermediate, path_final):
    
    if os.path.exists(path_intermediate) == True:
        os.remove(path_intermediate)
    if os.path.exists(path_final) == True:
        os.remove(path_final)
    print("Успех!")
    return True

clear_logs_folder(path_intermediate=jsonData['YaDirectApiConstant']["path_intermediate"],
    path_final=jsonData['YaDirectApiConstant']["path_final"])