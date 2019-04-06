import os
import cx_Oracle
import csv
import argparse
import configparser
import subprocess
import multiprocessing as mp


def send_alert(severity, ipaddr, hostname, platform, environment, status):
    msg_txt = (f"Alert Type:Ping_Control Hostnamme={hostname} "
               f"Ipaddress={ipaddr} Platform={platform} environment={environment} Status={status}  !!!")
    opcmsg_int_run = (f"opcmsg s={severity} a='{hostname}' o='{hostname}' msg_text='{msg_txt}' node=monitorserverp00")
    print(opcmsg_int_run)
    #opcmsg_int = os.system(opcmsg_int_run)


def read_csv(csvreadfile):
    with open(csvreadfile, newline='') as csvfile:
        csvlist = list(csv.reader(csvfile))
    return csvlist


def controlIP(ip):
    ipaddr = str(ip[0]).strip()
    hostname = str(ip[1]).strip()
    platform = str(ip[2]).strip()
    environment = str(ip[3]).strip()
    alertbasepath = '/tmp/test/'
    ExIPList = read_csv('excludeiplist')
    if ('None' not in str(ipaddr) and '.' in str(ipaddr) and ipaddr not in ExIPList[0]):
        if 'Test' in environment or 'Development' in environment:
            severity = 'Major'
        elif 'Production' in environment or 'PreProduction' in environment or 'Disaster' in environment:
            severity = 'Critical'
        else:
            severity = 'Minor'
        with open(os.devnull, "w") as fnull:
            if subprocess.call(['/bin/ping', "-c", "1", "-l", "1", "-s", "1", "-W", "1", str(ipaddr)],
                               stdout=fnull, stderr=fnull) == 0:
                alertfilepath = alertbasepath + severity + '_' + ipaddr
                ##check controlled ip address
                print('host %s is Up. Hostname : %s Platform: %s severity: %s' % (ipaddr, hostname, platform, "OK"))
                if os.path.isfile(alertfilepath):
                    print('host %s is UP-Alert Clear' % ipaddr)
                    #send_alert('Normal', ipaddr, hostname, platform, environment, 'Clear')
                    #subprocess.call(['/bin/rm', alertfilepath], stdout=fnull, stderr=fnull)
            else:
                alertfilepath = alertbasepath + severity + '_' + ipaddr
                if os.path.isfile(alertfilepath):
                    print('host %s is DOWN-Alert OK' % ipaddr)
                else:
                    print('host %s is DOWN-Alert Create. Server : %s admins: %s severity: %s' % (ipaddr, hostname, platform, severity))
                    #subprocess.call(['/usr/bin/touch', alertfilepath], stdout=fnull, stderr=fnull)
                    #send_alert(severity, ipaddr, hostname, platform, environment, 'Failed')


def parser_arg():
    parser = argparse.ArgumentParser(description="Define Configuration  file for Alert Control")
    parser.add_argument("-f", "--file", help="Define configuration  file  name", type=str)
    args = parser.parse_args()
    return args.file


def query_results(xtns, Username, Password, querystring):
    connection = cx_Oracle.connect(Username, Password, xtns)
    cursor = connection.cursor()
    cursor.execute(querystring)
    result = cursor.fetchall()
    return result


def main_function():
    config_file = parser_arg()
    if not config_file:
        print("HELP:Use  -f  option to define configuration file.\n"
              "     Ex: python PingCheck.py -f  <test_config>")
    else:
        config = configparser.ConfigParser()
        config.read(config_file)
        control_type = config.get('PARAM', 'CONTROL_TYPE')
        numberThreads = int(config.get('PARAM', 'NUMBERTHREADS'))
        iplistfile = config.get('PARAM', 'IPLIST')
        if control_type == 'ORACLE' or control_type == 'MYSQL' :
            xtns = config.get('DB', 'XTNS')
            username = config.get('DB', 'DB_USERNAME')
            password = config.get('DB', 'DB_PASSWORD')
            querystring = config.get('DB', 'QUERY')
            IPList = query_results(xtns, Username, Password, querystring)
            p = mp.Pool(numberThreads)
            IPout = p.map(controlIP, IPList)
            p.close()
            p.join()
        else:
            IPList = read_csv(iplistfile)
            p = mp.Pool(numberThreads)
            IPout = p.map(controlIP, IPList)
            p.close()
            p.join()


main_function()
