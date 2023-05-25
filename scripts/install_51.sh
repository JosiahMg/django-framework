#!bin/bash

NET_WORK="33"
Docker_net()
{
   route -n|grep 172.* |awk -F" " '{print $1}'|awk -F"." '{print $2}'|grep $NET_WORK  1>/dev/null 2>/dev/null
   if [ $? == 0 ];then
     echo  "algo-net already existed."
   else
     docker network create --driver bridge --subnet 172.${NET_WORK}.0.0/16 --gateway 172.${NET_WORK}.0.1 algo-net
     echo "algo-net create success."
   fi
}
Docker_net
docker-compose up trace_dev trace_test trace_alpha trace_qz_test trace_wx_test trace_xc_test -d