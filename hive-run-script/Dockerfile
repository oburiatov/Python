FROM ubuntu:16.04

RUN apt-get update 
RUN apt-get -y install libsasl2-dev python3 python3-pip 
RUN apt-get -y install build-essential
RUN apt-get -y install libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev
RUN apt-get -y install python3-pandas

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip3 install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python3", "main.py"]