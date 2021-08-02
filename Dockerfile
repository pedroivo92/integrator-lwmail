FROM python:3.8

RUN mkdir /opt/globo
WORKDIR /opt/globo

COPY . .

RUN apt-get update
RUN pip3 install -r requirements.txt


CMD [ "python", "main.py"]