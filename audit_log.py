import sys
import paramiko
import select
import termios
import tty
import socket
import logging
from paramiko.py3compat import u
import fcntl
import struct
def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('xxx.xx.xx.xx', username='root', password='aaaaaa')

    tran = ssh.get_transport()
    chan = tran.open_session()

    chan.get_pty()
    chan.invoke_shell()

    logging.basicConfig(level=logging.DEBUG,  
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',  
                    datefmt='%a, %d %b %Y %H:%M:%S',  
                    filename='test.log',  
                    filemode='w')  

    oldtty = termios.tcgetattr(sys.stdin)
    try:
        tty.setraw(sys.stdin.fileno())
        tty.setcbreak(sys.stdin.fileno())
        chan.settimeout(0.0)
        command = ''
        command_history = []
        tab = False
        up_tag = False
        down_tag = False
        while True:
            r, w, e = select.select([chan, sys.stdin], [], [])
            wat = fcntl.ioctl(r[0].fileno(), termios.FIONREAD, "  ")
            doublewat = struct.unpack('h', wat)[0]
            if chan in r:
                try:
                    recv_data = u(chan.recv(1024))
                    if len(recv_data) == 0:
                        sys.stdout.write("\r\n*** EOF\r\n")
                        break
                    if len(recv_data) == 1:
                        if recv_data in '\x07':
                            recv_data = recv_data.replace('\x07','')
                        if recv_data:
                            command += recv_data
                            logging.warn("accept one char command:" + command + "<--------------")
                    else:
                        if up_tag or down_tag:
                            command = recv_data
                            logging.debug("recv_data:" + recv_data + "..............")
                        else:
                            if '[' not in recv_data and ']' not in recv_data: 
                                if '\n' not in recv_data or '\r' not in recv_data:
                                    if tab:
                                        command += recv_data
                                    else:
                                        command = recv_data
                                    logging.error("command:"+str(tab)+command+"<-------")
                    sys.stdout.write(recv_data)
                    sys.stdout.flush()
                except socket.timeout:
                    pass
            if sys.stdin in r:
                x = sys.stdin.read(doublewat)
                if len(x) == 0:
                    break
                chan.send(x)
                input_char = x
                if input_char == '\t':
                    tab = True
                if input_char == '\x1b[A':
                    logging.debug("you pressed up|")
                    up_tag = True
                elif input_char == '\x1b[B':
                    logging.debug("you pressed down")
                    down_tag = True
                if input_char == '\x7F':
                    if command:
                        command = command[:-1]
                if input_char == '\x0D':
                    if command.strip():
                        if '\x07' in command or '\x08' in command:
                            command = command.replace('\x07','').replace('\x08','')
                        if '\r' or '\x1b[C' or '\x1b[K' in command:
                            command = command.replace('\r','').replace('\x1b[C','').replace('\x1b[K','')
                        command_history.append(command)
                    command = ''
                    tab = False
                    up_tag = False
                    down_tag = False
                    logging.debug(command_history)

    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, oldtty)
        ssh.close()
if __name__ == '__main__':
    main()
