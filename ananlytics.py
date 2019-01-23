import csv, sys
import os
from itertools import groupby


def read_csv(filename):
    data = []

    f = open(filename, 'r')
    reader = csv.reader(f)
    for row in reader:
        # print(row)
        data.append(row)
    return [x[0].split(';') for x in data]

def get_top_frequency(data):
    result = {}
    for item in data:
        if item in result:
            result[item] = result[item] + 1
        else:
            result[item] = 1

    result_list = []
    for x, y in result.items():
        result_list.append([x, y])

    return sorted(result_list,key=lambda x:-x[1])

def sort_based_on_second(data):
    return sorted(data,key=lambda x:-float(x[1]))

def general_analytics(data):
    print(data[:2])

    # top 20 apps
    app_name_list = [x[0] for x in data[1:]]
    # print(app_name_list[:20])
    ranking_apps = get_top_frequency(app_name_list)
    for r in ranking_apps[:20]:
        print(r[0])
        # print('"' + r[0] + '"' + ',')

    # longest runtime
    # app_with_run_time_list = [[x[0],x[7]] for x in data[1:]]
    # ranking_acutal_time = sort_based_on_second(app_with_run_time_list)
    # for r in [[x[0],int(float(x[1]))] for x in ranking_acutal_time[:20]]:
    #     print (r[0])

    # longest waiting time
    # app_with_run_time_list = [[x[0],x[8]] for x in data[1:]]
    # ranking_acutal_time = sort_based_on_second(app_with_run_time_list)
    # for r in ranking_acutal_time[:20]:
    #     print (r[1])

    return

def detailed_analytics(data, app_name):
    # print(data[0])

    # print (app_name)

    data_for_app = [x for x in data if app_name in x[0]]

    # for x in data_for_app[:10]:
    #     print(x)
    # commom config
    data_config = [x[1]+' '+x[2]+' '+x[3]+' '+x[4]+' '+x[5] for x in data_for_app]
    # ranking_apps = [[x[0].split(" ", 1)[1],x[1]] for x in get_top_frequency(data_config)]
    ranking_apps = [[x[0],x[1]] for x in get_top_frequency(data_config)]


    # data size
    len_of_app = len(data_for_app)
    if len_of_app > 0:
    # print(len_of_app)
        data_read_bytes = sum([float(x[9]) for x in data_for_app])/len_of_app
        data_shuffle_read_bytes = sum([float(x[10]) for x in data_for_app])/ len_of_app
        data_shuffle_write_bytes = sum([float(x[11]) for x in data_for_app])/len_of_app

    # actual runtime
    run_time = [float(x[7]) for x in data_for_app if float(x[7])>0]
    # print(run_time)
    acutal_runtime = 0
    if len(run_time) != 0:
        acutal_runtime = sum(run_time)/len(run_time)
    print(acutal_runtime)

    # print([float(x[7]) for x in data_for_app])

    # # print(app_name)
    # if len(ranking_apps)<=5:
    #     print(ranking_apps)
    # else:
    #     print(ranking_apps[:5])
    # print(ranking_apps[0])

    # print('input_bytes, shuffle_read_bytes, shuffle_write_bytes')
    # print(data_read_bytes,data_shuffle_read_bytes,data_shuffle_write_bytes)

    # throughput = 0
    # if acutal_runtime > 0:
    #     throughput = data_read_bytes / acutal_runtime
    # print(throughput)

    return


filename = 'result/result_v1_new.csv'
# general_analytics(read_csv(filename))

# app_names = ['WW/web_launcher','email_monitor/datahighway-etl.jar','ft_launcher','car_email','repair_launcher',
#              'ITSample_Buffer','verify_launcher','carservice_report','carservice','standard_batch_launcher',
#              'standard_batch_launcher.jar', 'datahiway_result_kirin/datahighway-etl.jar', 'datahiway_kirin/datahighway-etl.jar','datahiway_simple/datahighway-etl.jar','zd_risk_sn_item4zmodel.jar',
#              'carservice/failnum.jar', 'carservice/carservice.jar', 'ITSample/web_launcher.jar', 'likeyu/LawSample.jar', 'test/zd_risk_sn_item.jar'
#              ]

app_names = ["MonitorDelay_kafka_save_date",
                "SampleMaster WW WebCancelLauncher",
                "SampleMaster WW WebRegisterLauncher",
                "CarFormService",
                "SampleMaster WW RepairRegisterSNVerifyLauncher",
                "SampleMaster WW RepairCandidateSNVerifyLauncher",
                "monitorStream_tm_ft",
                "Spark BtchCstrWeb",
                "SampleMaster WW FTTestRegisterSNVerifyLauncher",
                "SampleMaster WW FTTestCandidateSNVerifyLauncher",
                "SampleMaster_ZD CandidateSNLauncher",
                "sparkuser_bar",
                "SampleMaster WW FTTestDeltaSNVerifyLauncher",
                "Association_GetAtlanta_kirin_wrn_result",
                "Insert_GetAtlanta_kirin_wrn_btch",
                "Send_Batch_Date_SnInfo",
                "SampleMaster ZD RiskSNItemForZModelRiskLauncher",
                "IT EmailMain",
                "ITSampleMaster CarMain",
                "Spark LawSample",]

app_names = ['MonitorDelay_kafka_save_date']
extracted_data = read_csv(filename)
for app_name in app_names:
    detailed_analytics(extracted_data,app_name)