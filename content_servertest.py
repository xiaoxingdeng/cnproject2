import socket
import threading
import time

BUFSIZE = 1024  # size of receiving buffer
SERVER_HOST = socket.gethostbyname(socket.gethostname())
configdict = {}

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


def detectalive_single(key,ip, port):
    global configdict
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    send_data = 'Liveness'
    s.settimeout(0.5)
    s.sendto(send_data.encode('utf-8'), (ip, port))
    neighbors = configdict['neighbors']
    entry = neighbors[key]
    try:
        data, address = s.recvfrom(BUFSIZE)
        string_data = data.decode('utf-8')
        if string_data != entry[5]:
            entry[5] = string_data
        entry[4] = 0
        print(entry[5], 'is connected')
    except socket.timeout:
        if entry[4] < 3:
            entry[4] += 1
        else:
            print(entry[5], 'is not connected')


def detectalive_all():
    neighbors = configdict["neighbors"]
    while True:
        time.sleep(3)
        for key, item in neighbors.items():
            hostname = item[1]
            ip = socket.gethostbyname(hostname)
            port_char = item[2]
            port = int(port_char)
            thread_client = threading.Thread(target=detectalive_single, args=(key, ip, port))
            thread_client.start()

def print_allneighbors():
    neighbors = configdict["neighbors"]
    printneighbors = {}
    for value in neighbors.values():
        if value[4] <3:
            entry = {"uuid": value[0], "host": value[1], "backend_port": value[2], "metric": value[3]}
            printneighbors[value[5]] = entry
    printobject = {}
    printobject["neighbors"] = printneighbors
    print(printobject)

if __name__ == '__main__':
    open_config('node1.conf', configdict)
    SERVER_PORT = configdict["backend_port"]
    # start the server to listen
    threadserver = threading.Thread(target=openserver, args=(SERVER_PORT,))
    threadserver.start()
    # detect the liveness of neighbors
    thread_detectalive = threading.Thread(target=detectalive_all)
    thread_detectalive.start()

    while True:
        time.sleep(3)
        print_allneighbors()

