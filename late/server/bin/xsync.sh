!/bin/bash
# $#：表示传递给脚本或函数的参数个数。
#1 获取输入参数个数，如果没有参数，直接退出
pcount=$#
if((pcount==0)); then
echo no args;
exit;
fi
 
#2 获取文件名称
p1=$1
fname=`basename $p1`
echo fname=$fname
 
#3 获取上级目录到绝对路径
 
#4 获取当前用户名称

 
#5 循环
for host in {hp50}; do
        #echo $pdir/$fname $user@192.168.168.2$host:$pdir
       echo --------------- 192.168.168.2$host ----------------
      sshpass -p !@#linux123 rsync -rvl ~/etcd-redis-all/contrib/late/server/bin/$fname $host@192.168.168.2$host:/home/$host/zgd/lateTest/
done
#sshpass -p !@#linux123 rsync -rvl /home/hp21/zgd/$fname hp2$host@192.168.168.2$host:/home/hp2$host/zgd/
#219.216.65.234:20050
 
#5 循环
#for host in {master,slaver1,slaver2,slaver3,slaver4,slaver5}; do
#        #echo $pdir/$fname $user@$host:$pdir
#        echo --------------- $host ----------------
#        rsync -rvl $pdir/$fname $user@$host:$pdir
#done

