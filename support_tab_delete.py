import sys
import paramiko
import select
import termios
import tty
import socket
import logging
from paramiko.py3compat import u
def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('10.19.126.138', username='root', password='MhxzKhl')

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
        out = open("output.log", "a+")
        inp = open("input.log","a+")
        cmd_record = []
        command = ''
        command_history = []
        cmd_length = 0
        tab = False
        while True:
            r, w, e = select.select([chan, sys.stdin], [], [])
            if chan in r:
                try:
                    recv_data = u(chan.recv(1024))
                    logging.debug("recv_data:"+recv_data+'<------>')
                    if len(recv_data) == 0:
                        sys.stdout.write("\r\n *** EOF\r\n")
                        break 
                    if len(recv_data) == 1:
                        #if recv_data != '\x08':
                        if recv_data in '\x07':
                            recv_data = recv_data.replace('\x07','')
                        if recv_data:
                            command += recv_data
                            logging.debug("accept one char command:" + command)
                    else:
                        if '[' not in recv_data and ']' not in recv_data: 
                            if '\n' not in recv_data or '\r' not in recv_data:
                                if tab:
                                    if '\x07' in recv_data:
                                        recv_data = recv_data.replace('\x07','')
                                    command += recv_data
                                else:
                                    command = recv_data
                                logging.debug("command:"+str(tab)+command+">")

                    sys.stdout.write(recv_data)
                    sys.stdout.flush()
                except socket.timeout:
                    pass
            if sys.stdin in r:
                arrow = []
                x = sys.stdin.read(1)
                if len(x) == 0: 
                    break
                if ord(x) == 27:
                    arrow.append(x)
                    x1 = sys.stdin.read(1)
                    if ord(x1) == 91:
                        arrow.append(x1)
                    arrow.append(sys.stdin.read(1))
                    x = ''.join(arrow)
                chan.send(x)
                input_char = x
                if input_char == '\t':
                    tab = True
                if input_char == '\x7F':
                    if command:
                        command = command[:-1]
                if input_char == '\x0D':
                    logging.debug("you pressed enter>>>>")
                    if command.strip():
                        command_history.append(command)
                        command = ''
                        tab = False
                    logging.debug(command_history)
                else:
                    pass
        inp.close()
        out.close()
    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, oldtty)
        ssh.close()
if __name__ == '__main__':
	main()
