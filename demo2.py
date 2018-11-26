        cmd_record = []
        command = ''
        command_history = []
        while True:
            r, w, e = select.select([chan, sys.stdin], [], [])
            if chan in r:
                try:
                    recv_data = u(chan.recv(1024))
                    if len(recv_data) == 0:
                        sys.stdout.write("\r\n *** EOF\r\n")
                        break 
                    elif len(recv_data) == 1:
                        #if recv_data != '\b':
                        cmd_record.append(recv_data)
                    elif '[' not in recv_data and ']' not in recv_data: 
                        if '\n' not in recv_data and '\r\n' not in recv_data:
                            out.write(recv_data)
                    else:
                        if cmd_record:
                            out.write(''.join(cmd_record)+'\n')
                            cmd_record = []
                    sys.stdout.write(recv_data)
                    sys.stdout.flush()
                except socket.timeout:
                    pass
