import socket
import threading
import time
import argparse

BUFSIZE = 1024  # size of receiving buffer
SERVER_HOST = socket.gethostbyname(socket.gethostname())
configdict = {}
mapdict = {"map": {}}
messagedict = {}


def open_config(filename, configdict):
    with open(filename, "r") as f:
        neighbors = {}
        data = f.read()
        newlines = data.splitlines()
        for line in newlines:
            newline = line.split(" = ")
            if newline[0][0:4] == "peer":
                if newline[0] == 'peer_count':
                    configdict[newline[0]] = int(newline[1])
                else:
                    data = newline[1].split(", ")
                    data.append(3)
                    data.append("")
                    data.append(0)
                    neighbors[newline[0]] = data
            elif newline[0] == 'backend_port':
                configdict[newline[0]] = int(newline[1])
            else:
                configdict[newline[0]] = newline[1]
        configdict["neighbors"] = neighbors


def openserver(SERVER_PORT):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_address = (SERVER_HOST, SERVER_PORT)
    s.bind(server_address)
    print(f"[LISTENNING] Server is listenning on  {server_address}")
    while True:
        data, address = s.recvfrom(BUFSIZE)
        string_data = data.decode('utf-8')
        if string_data == 'Liveness':
            send_data = configdict["name"]
            s.sendto(send_data.encode('utf-8'), address)
        elif string_data[0:8] == 'Add_dict':
            infor = string_data.split(' ')
            infordict = {}
            infordict['uuid'] = infor[1]
            infordict['host'] = infor[2]
            infordict['backend_port'] = infor[3]
            infordict['metric'] = infor[4]
            add_adddict(infordict)
            send_data = 'Successfully add'
            s.sendto(send_data.encode('utf-8'), address)
        elif string_data[0:7] == 'Map_msg':
            handle_message(string_data)

def handle_message(string_data):
    global mapdict
    datalist=string_data.split(" ", 3)
    if datalist[1] in messagedict:
        if messagedict[datalist[1]] >= int(datalist[2]):
            return
    messagedict[datalist[1]] = int(datalist[2])
    md = eval(datalist[3])
    if md != mapdict:
        mapdict = md
        update_mystate()
        broadcast_map()







def detectalive_single(key):
    global configdict
    neighbors = configdict['neighbors']
    entry = neighbors[key]
    hostname = entry[1]
    ip = socket.gethostbyname(hostname)
    port_char = entry[2]
    port = int(port_char)

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    send_data = 'Liveness'
    s.settimeout(0.5)
    s.sendto(send_data.encode('utf-8'), (ip, port))
    try:
        data, address = s.recvfrom(BUFSIZE)
        string_data = data.decode('utf-8')
        if string_data != entry[5]:
            entry[5] = string_data
        if entry[4] != 0:
            thread_client = threading.Thread(target=send_dict_confirm, args=(entry,))
            thread_client.start()
        if entry[4] == 3:
            update_mystate()
            entry[4] = 0
        else:
            entry[4] = 0
    except socket.timeout:
        if entry[4] < 3:
            if entry[4] == 2:
                update_mystate()
                entry[4] += 1
            else:
                entry[4] += 1
    s.close()

def send_dict_confirm(entry):
    ip = socket.gethostbyname(entry[1])
    port = int(entry[2])
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    send_data = 'Add_dict' + ' ' + configdict['uuid'] + ' ' + socket.gethostname() + ' ' + str(
        configdict['backend_port']) + ' ' + entry[3]
    s.settimeout(0.5)
    while True:
        try:
            s.sendto(send_data.encode('utf-8'), (ip, port))
            data, address = s.recvfrom(BUFSIZE)
            break
        except socket.timeout:
            time.sleep(3)
            continue
    s.close()

def detectalive_all():
    neighbors = configdict["neighbors"]
    while True:
        time.sleep(3)
        for key, item in neighbors.items():
            thread_client = threading.Thread(target=detectalive_single, args=(key,))
            thread_client.start()


def print_allneighbors():
    neighbors = configdict["neighbors"]
    printneighbors = {}
    for value in neighbors.values():
        if value[4] < 3:
            entry = {"uuid": value[0], "host": value[1], "backend_port": value[2], "metric": value[3]}
            printneighbors[value[5]] = entry
    printobject = {}
    printobject["neighbors"] = printneighbors
    print(printobject)



def send_adddict(infordict):
    ip = socket.gethostbyname(infordict['host'])
    port = int(infordict['backend_port'])
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    send_data = 'Add_dict' + ' ' + configdict['uuid'] + ' ' + socket.gethostname() + ' ' + str(
        configdict['backend_port']) + ' ' + infordict['metric']
    s.settimeout(0.5)
    while True:
        try:
            s.sendto(send_data.encode('utf-8'), (ip, port))
            data, address = s.recvfrom(BUFSIZE)
            break
        except socket.timeout:
            time.sleep(3)
            continue
    s.close()


def add_adddict(infordict):
    global configdict
    for entry in configdict["neighbors"].values():
        if entry[0] == infordict['uuid']:
            return
    newentry = []
    newentry.append(infordict['uuid'])
    newentry.append(infordict['host'])
    newentry.append(infordict['backend_port'])
    newentry.append(infordict['metric'])
    newentry.append(3)
    newentry.append('')
    newentry.append(0)
    configdict['peer_count'] += 1
    configdict['neighbors']['peer_' + str(configdict['peer_count'])] = newentry
    update_mystate()


def add_neighbor(addmessage):
    information = addmessage.split(' ')
    infordict = {}
    for infor in information:
        splitinfor = infor.split('=')
        infordict[splitinfor[0]] = splitinfor[1]
    add_adddict(infordict)
    thread_send_adddict = threading.Thread(target=send_adddict, args=(infordict,))
    thread_send_adddict.start()


def update_mystate():
    global mapdict
    map = mapdict['map']
    mystatedict = {}
    neighbors = configdict["neighbors"]
    for value in neighbors.values():
        if value[4] < 3:
            mystatedict[value[5]] = value[3]
        elif value[4] == 3:
            if value[5] in map:
                del map[value[5]]
    map[configdict['name']] = mystatedict

def broadcast_map():
    neighbors = configdict["neighbors"]
    for key, item in neighbors.items():
        thread_client = threading.Thread(target=send_map, args=(key,))
        thread_client.start()

def send_map(key):
    global configdict
    neighbors = configdict['neighbors']
    entry = neighbors[key]
    hostname = entry[1]
    ip = socket.gethostbyname(hostname)
    port_char = entry[2]
    port = int(port_char)

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    update_mystate()
    send_data = 'Map_msg' + ' ' + configdict['name'] + ' ' + str(entry[6]) + ' ' + str(mapdict)
    entry[6] += 1
    s.sendto(send_data.encode('utf-8'), (ip, port))
    s.close()



if __name__ == '__main__':
    arg_finder = argparse.ArgumentParser()
    arg_finder.add_argument('-c', required=True, type=str)
    args = arg_finder.parse_args()

    open_config(args.c, configdict)
    SERVER_PORT = configdict["backend_port"]
    # start the server to listen
    threadserver = threading.Thread(target=openserver, args=(SERVER_PORT,))
    threadserver.start()
    # detect the liveness of neighbors
    thread_detectalive = threading.Thread(target=detectalive_all)
    thread_detectalive.start()

    # print_allneighbors()
    if (args.c == 'node0.conf'):
        add_neighbor(
            "uuid=24f22a83-16f4-4bd5-af63-9b5c6e979dbb host=dengxiaohangdeMacBook-Pro.local backend_port=18346 metric=10")
        add_neighbor(
            "uuid=c317c8d2-a72b-11ec-b909-0242ac120002 host=dengxiaohangdeMacBook-Pro.local backend_port=19666 metric=20")

    while True:
        time.sleep(3)
        update_mystate()
        broadcast_map()
        print(mapdict)
