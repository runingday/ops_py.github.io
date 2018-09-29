### python获取挂载点信息
前提需要安装python-dev python36-dev（python36)

```
pip install psutil


import psutil
partitions = psutil.disk_partitions()

for p in partitions:
    print p.mountpoint, psutil.disk_usage(p.mountpoint).percent

```
