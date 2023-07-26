FROM initial

WORKDIR /srv/listener
COPY . .
RUN pip3 install -r requirements.txt

ENTRYPOINT [ "python3", "run.py" ]
# ENTRYPOINT [ "tail", "-f", "/dev/null" ]