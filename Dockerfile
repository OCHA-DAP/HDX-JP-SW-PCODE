FROM python:3.10-alpine

WORKDIR /srv/listener

COPY . .

RUN apk add git g++ gfortran musl-dev gdal-dev proj-util proj-dev geos-dev

RUN pip3 install -r requirements.txt

# ENTRYPOINT [ "python3", "listen.py" ]
ENTRYPOINT [ "tail", "-f", "/dev/null" ]