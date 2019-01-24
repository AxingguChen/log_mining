import json
import os
import csv

#global
oneM = 1000000      #from bytes to mb
oneK = 1000         #from ms to s

# input: json list
# output: job jar name
def get_job_name(json_content):
    if len(json_content)!=0:
        # configurations = [x['System Properties']['sun.java.command'] for x in json_content if
        #    x['Event'] == 'SparkListenerEnvironmentUpdate'][0]
        # # print (configurations.split(" ")[-1])
        # return configurations.split(" ")[-1]

        spark_app_name = [x['Spark Properties']['spark.app.name'] for x in json_content if
                          x['Event'] == 'SparkListenerEnvironmentUpdate'][0]
        return spark_app_name

    else:
        print('no such job')


# input: json list
# output: resource configuration [driver cores, driver memory, executor number, executor vcore, executor memoery]
def get_configuration(json_content):
    drive_m = 0
    drive_v = 0
    executor_m = 0
    executor_v = 0
    executor_num = 0
    if len(json_content)!=0:
        configurations = [x['System Properties']['sun.java.command'] for x in json_content if
           x['Event'] == 'SparkListenerEnvironmentUpdate'][0].split(" ")
        print(configurations)
        for i in range(len(configurations)):
            if 'spark.driver.memory' in configurations[i]:
                drive_m = configurations[i].split('=')[-1]
            elif '--driver-cores' in configurations[i]:
                drive_v = configurations[i+1]
            elif 'executor-memory' in configurations[i]:
                executor_m = configurations[i+1]
            elif 'executor-cores' in configurations[i]:
                executor_v = configurations[i+1]
            elif 'num-executor' in configurations[i]:
                executor_num = configurations[i+1]
    else:
        print('no such job')

    return drive_v, drive_m, executor_num, executor_v, executor_m

# application info
# input: json list
# output: application elapsed time, submission time, completion time
def time_elapse(json_content):
    elapsed_time = 0
    if len(json_content)!=0:
        # app_logs[0] for start app_log[1] for end
        app_logs = [x for x in json_content if
           x['Event'] == 'SparkListenerApplicationStart' or x['Event'] == 'SparkListenerApplicationEnd']
        elapsed_time = (app_logs[1]['Timestamp'] - app_logs[0]['Timestamp'])/oneK
        # print (app_logs)
    else:
        print('no such job')
    return [elapsed_time, app_logs[0]['Timestamp'], app_logs[1]['Timestamp']]

# job info
# input: json list
# output: job info
def job_info(json_content):

    #job_id, job_result, time_elapse, delay, stage_IDs
    jobs_status = []
    if len(json_content)!=0:
        jobs = []
        # job_logs odd for start even for end
        job_logs = [x for x in json_content if
           x['Event'] == 'SparkListenerJobStart' or x['Event'] == 'SparkListenerJobEnd']
        for i in range(int(len(job_logs)/2)):

            if len([x for x in job_logs if x['Job ID']== i and x['Event'] == 'SparkListenerJobStart'])>0 and \
                         len([x for x in job_logs if x['Job ID'] == i and x['Event'] == 'SparkListenerJobEnd'])>0:
                jobs.append([[x for x in job_logs if x['Job ID']== i and x['Event'] == 'SparkListenerJobStart'][0],
                         [x for x in job_logs if x['Job ID'] == i and x['Event'] == 'SparkListenerJobEnd'][0]])

        for i in range(len(jobs)):
            analysis = []

            analysis.append(jobs[i][0]['Job ID'])

            analysis.append(jobs[i][1]['Job Result']['Result'])
            analysis.append((jobs[i][1]['Completion Time'] - jobs[i][0]['Submission Time'])/oneK)

            if jobs[i][0]['Job ID'] == 0:
                analysis.append((jobs[i][0]['Submission Time'] - time_elapse(json_content)[1])/oneK) #time_elapse(json_content) [elapsed_time submission_time complertion_time]
            else:
                analysis.append((jobs[i][0]['Submission Time']-jobs[i-1][0]['Submission Time'])/oneK)

            analysis.append(jobs[i][0]['Stage IDs'])

            jobs_status.append(analysis)

    else:
        print('no such job')

    return jobs_status


# stage and task info
# input: json list
# output: job info
def stage_task_info(json_content, jobs_status, job_id):

    #[stage_id, stage_time_elapse, input bytes, shuffle read bytes, shuffle write bytes]
    stages_status = []

    if len(jobs_status)!=0:
        job_status = [x for x in jobs_status if x[0]==job_id][0]
        stage_IDs = job_status[4]
        for i in range(len(stage_IDs)):
            input_read_bytes = 0
            shuffle_read_bytes = 0
            shuffle_write_bytes = 0

            stage = []

            stage.append(stage_IDs[i])

            stage_info_empty_not = [x for x in json_content if x['Event']=='SparkListenerStageCompleted' and x['Stage Info']['Stage ID'] == stage_IDs[i]]
            if len(stage_info_empty_not) !=0:
                stage_info = stage_info_empty_not[0]

                stage.append((stage_info['Stage Info']["Completion Time"]-stage_info['Stage Info']["Submission Time"])/oneK)

                tasks_info = [x for x in json_content if x['Event']=='SparkListenerTaskEnd' and x['Stage ID'] == stage_IDs[i]]

                input_read = [x['Task Metrics']['Input Metrics']['Bytes Read']  for x in tasks_info if 'Task Metrics' in x and 'Input Metrics' in x['Task Metrics']]
                if len(input_read)!=0:
                    input_read_bytes = input_read_bytes + sum(input_read)

                shuffle_read = [[x['Task Metrics']['Shuffle Read Metrics']['Remote Bytes Read'],x['Task Metrics']['Shuffle Read Metrics']['Local Bytes Read']] for x in tasks_info if 'Task Metrics' in x and 'Shuffle Read Metrics' in x['Task Metrics']]
                if len(shuffle_read)!=0:
                    remote_bytes = sum([x[0] for x in shuffle_read])
                    local_bytes = sum([x[1] for x in shuffle_read])
                    shuffle_read_bytes = shuffle_read_bytes + remote_bytes + local_bytes

                shuffle_write = [x['Task Metrics']['Shuffle Write Metrics']['Shuffle Bytes Written'] for x in tasks_info if 'Task Metrics' in x and 'Shuffle Write Metrics' in x['Task Metrics']]
                if len(shuffle_write)!=0:
                    shuffle_write_bytes =  shuffle_write_bytes + sum(shuffle_write)

                # print('bytes inutp')
                # print(input_read_bytes)
                # print(shuffle_read_bytes)
                # print(shuffle_write_bytes)

                stage.append(input_read_bytes)
                stage.append(shuffle_read_bytes)
                stage.append(shuffle_write_bytes)

                stages_status.append(stage)
    else:
        print('no job, neither stage')


    # print (stages_status)
    return stages_status

# input: file path
# output load the txt in json list form
def read_txt_to_json(filename):
    exists = os.path.isfile(filename)
    if exists:
        #read into json
        print (filename + ' file exists')
        with open(filename) as txt_file:
            txt_content = txt_file.readlines()

        json_content = [json.loads(x) for x in txt_content]
        return json_content
    else:
        print (filename + ' file does not exist')
        return []


def loop_logs(folder,file_prefix,log_id, howmany,result_file):
    cnt = 0

    file = open(result_file, 'w')
    with file:
        writer = csv.writer(file,delimiter=';')
        header = ['app_name', 'driver_core', 'driver_memory', 'executor_num',
                 'executor_core', 'executor_memory', 'application_time_elapse(s)',
                  'jobs_actual_elapse(s)', 'jobs_delay(s)', 'max job input bytes(mb)',
                  'max job shuffle read (mb)', 'max job shuffle write (mb)',
                  'jobs(job_id,status,time_elapsed,delay,stage_IDs)', 'filename']
        writer.writerow(header)


        for i in range (howmany):
            flag = False

            while not flag:
                filename = folder + file_prefix + str(log_id+cnt) + '.txt'
                cnt = cnt + 1
                json_content = read_txt_to_json(filename)
                if len(json_content)==0:
                    continue
                else:
                    flag = True
                    print('************')

                    print('application name: ')
                    app_name = get_job_name(json_content)
                    print(app_name)

                    driver_core, driver_memory, executor_num, executor_core, executor_memory = get_configuration(json_content)

                    print('application elapsed: ')
                    time_elapsed =  time_elapse(json_content)[0]
                    print(time_elapsed)

                    actual_elapsed = 0

                    input_read_bytes = [0]
                    shuffle_read_bytes = [0]
                    shuffle_write_bytes = [0]

                    print('jobs status ')
                    jobs_status = job_info(json_content)
                    for i in range(len(jobs_status)):
                        print('job: ' + str(i))
                        print('job id: ' + str(jobs_status[i][0]))
                        print('job result: ' + jobs_status[i][1])
                        print('job elapsed: ' + str(jobs_status[i][2]))
                        print('job delay: ' + str(jobs_status[i][3]))
                        actual_elapsed = actual_elapsed + jobs_status[i][2]
                        # delay = delay + job_status[i][2]

                        stage_status = stage_task_info(json_content, jobs_status, jobs_status[i][0])
                        input_read_bytes.append(sum([x[2] for x in stage_status]))
                        shuffle_read_bytes.append(sum([x[3] for x in stage_status]))
                        shuffle_write_bytes.append(sum([x[4] for x in stage_status]))


                    print('job actual run time: ' +str(actual_elapsed))

                    delay = time_elapsed - actual_elapsed



                    line = [app_name, str(driver_core), str(driver_memory), str(executor_num),
                            str(executor_core), str(executor_memory), str(time_elapsed), str(actual_elapsed),
                            str(delay), str(max(input_read_bytes)/oneM), str(max(shuffle_read_bytes)/oneM),
                            str(max(shuffle_write_bytes)/oneM), str(jobs_status), file_prefix + str(log_id+cnt-1) + '.txt']

                    writer.writerow(line)

                    print('!!!!!!!!!!!!')

                #in case infinite loop
                if cnt == 10000:
                    break

    file.close()
    return

file_prefix = 'application_1539329557024_'

#input1
folder = 'sparklog20190122-2/'
log_id  = 983548
file_to_read = 600
result_file = 'result/result_v3-2.csv'

#input
# folder = 'sparklog_100119/'
# log_id  = 869229
# file_to_read = 480
# result_file = 'result/result_v2.csv'
loop_logs(folder,file_prefix,log_id, file_to_read,result_file)
# print(job_info(read_txt_to_json(filename)))
