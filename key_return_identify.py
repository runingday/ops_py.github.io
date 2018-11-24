while True:
            r, w, e = select.select([chan, sys.stdin], [], [])
            if chan in r:
                try:
                    x = u(chan.recv(1024))
                    if len(x) == 0:
                        sys.stdout.write("\r\n*** EOF\r\n")
                        break
                    #这里判断是否是return
                    elif len(x) == 2:
                        if ord(x[:-1]) == 13:
                            fp.write("you pressed return\n")
                    sys.stdout.write(x)
                    sys.stdout.flush()
                except socket.timeout:
                    pass
            if sys.stdin in r:
                x = sys.stdin.read(1)
                if len(x) == 0:
                    break
                cmd = []
                if ord(x) == 27:
                    cmd.append(x)
                    cmd.append(sys.stdin.read(2))
                    x = ''.join(cmd)
                    #fp.write("you pressed a up/down/left/right key\n")
                chan.send(x)
