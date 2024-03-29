# 基础镜像为python3.9
FROM python:3.9
# 使用root用户
#USER root
## 设置为国内apt源
RUN sed -i s/deb.debian.org/mirrors.aliyun.com/g /etc/apt/sources.list.d/debian.sources
# 更新apt
RUN apt-get update && \
    apt-get install -y vim --no-install-recommends && \
    rm -rfv /var/lib/apt/lists/*

# 阿里云平台的python3.9.18 该镜像安装了以上vim gcc
# FROM registry.cn-hangzhou.aliyuncs.com/3040/python:3.9


# 在镜像中创建目录，用来存放本机中的django项目
RUN mkdir /usr/src/app
# 将本机 . 也就是当前目录下所有文件都拷贝到image文件中指定目录
COPY . /usr/src/app
# 将/usr/src/app指定为工作目录
WORKDIR /usr/src/app

# 在image中安装运行django项目所需要的依赖
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple/ -r requirements.txt --no-cache-dir

# 加密
#RUN encryptpy init .
#WORKDIR /usr/src
#RUN rm -rf app && \
#    mv build app
#WORKDIR /usr/src/app

# 开放容器的8000端口，允许外部链接这个端口
EXPOSE 18072

# 启动命令
#CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]

RUN chmod u+x ./scripts/start.sh
CMD ["sh", "./scripts/start.sh"]