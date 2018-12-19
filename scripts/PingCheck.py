import os
import cx_Oracle
import subprocess
import multiprocessing as mp


def send_alert(severity, ipaddr, hostname, platform, environment, status):
    msg_txt = (f"Alert Type:Ping_Control Hostnamme={hostname} "
               f"Ipaddress={ipaddr} Platform={platform} environment={environment} Status={status}  !!!")
    opcmsg_int_run = (f"opcmsg s={severity} a='{hostname}' o='{hostname}' msg_text='{msg_txt}' node=monitorserverp00")
    print(opcmsg_int_run)
    #opcmsg_int = os.system(opcmsg_int_run)
def controlIP(ip):
    ipaddr = str(ip[1]).strip()
    hostname = str(ip[0]).strip()
    platform = str(ip[2]).strip()
    environment = str(ip[3]).strip()
    alertbasepath = '/appdata/Control/PING/output/'
    if ('None' not in str(ipaddr) and '.' in ipaddr):
        if 'Test' in environment or 'Development' in environment:
            severity = 'Major'
        elif 'Production' in environment or 'PreProduction' in environment or 'Disaster' in environment:
            severity = 'Critical'
        with open(os.devnull, "w") as fnull:
            if subprocess.call(['/bin/ping', "-c", "1", "-l", "1", "-s", "1", "-W", "1", str(ipaddr)],
                               stdout=fnull, stderr=fnull) == 0:
                alertfilepath = alertbasepath + severity + '_' + ipaddr
                if os.path.isfile(alertfilepath):
                    print('host %s is UP-Alert Clear' % ipaddr)
                    send_alert('Normal', ipaddr, hostname, platform, environment, 'Clear')
                    subprocess.call(['/bin/rm', alertfilepath], stdout=fnull, stderr=fnull)
            else:
                alertfilepath = alertbasepath + severity + '_' + ipaddr
                if os.path.isfile(alertfilepath):
                    print('host %s is DOWN-Alert OK' % ipaddr)
                else:
                    print('host %s is DOWN-Alert Create' % ipaddr)
                    subprocess.call(['/usr/bin/touch', alertfilepath], stdout=fnull, stderr=fnull)
                    send_alert(severity, ipaddr, hostname, platform, environment, 'Failed')


def query_results(xtns, Username, Password, querystring):
    connection = cx_Oracle.connect(Username, Password, xtns)
    cursor = connection.cursor()
    cursor.execute(querystring)
    result = cursor.fetchall()
    return result


xtns = '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(Host=192.168.230.101)(Port=1521))(CONNECT_DATA=(SID=DBUCM)))'
Username = 'Test'
Password = 'Test!'
querystring = 'SELECT A_DISPLAY_LABEL HOSTNAME,A_IP_ADDRESS_VALUE IP,A_PLATFORMS_X PLATFORM, A_ENVIRONMENT_X ENVIRONMENT from UCMDB.CDM_UNIX_1 WHERE A_IP_ADDRESS_VALUE IS NOT NULL or  A_DISPLAY_LABEL IS NOT NULL'
numberThreads = 12

#IPList = query_results(xtns, Username, Password, querystring)
IPList = ([['bkmallinone1',  '',  'Ticketis', 'Production'],
           ['bkmallinone2',  '8.8.4.4',  'Ticketis', 'Production'],
           ['bkmallinone3',  '192.168.180.2',  'Ticketis', 'Test'],
           ['bkmallinone4',  '92.168.180.3',  'Ticketis', 'Test'],
           ['bkmallinone5',  '8.8.8.2',  'Ticketis', 'Production']])

p = mp.Pool(numberThreads)
IPout = p.map(controlIP, IPList)
p.close()
p.join()
