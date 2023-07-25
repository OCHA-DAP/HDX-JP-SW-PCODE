FROM public.ecr.aws/unocha/python:3.10

WORKDIR /srv/listener

COPY . .

# RUN apk add --no-cache git g++ gfortran proj-util && \
#     apk add --no-cache --virtual .build-deps python3-dev proj-dev geos-dev musl-dev gdal-dev && \
#     pip3 install -r requirements.txt && \
#     apk del .build-deps

RUN apk add --no-cache git g++ gfortran proj-util && \
    apk add --no-cache --virtual .build-deps python3-dev proj-dev geos-dev musl-dev gdal-dev && \
    pip3 install -r initial-requirements.txt
COPY . .
RUN pip3 install -r requirements.txt

ENTRYPOINT [ "python3", "run.py" ]
# ENTRYPOINT [ "tail", "-f", "/dev/null" ]