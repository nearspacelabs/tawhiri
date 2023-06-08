FROM python:3.8-slim

RUN apt-get update && apt-get install -y git gcc tini
WORKDIR /root

COPY requirements.txt requirements.txt
COPY setup.py setup.py
COPY tawhiri tawhiri
COPY scripts scripts

RUN pip3 install --user --ignore-installed --no-warn-script-location -r requirements.txt && \
  python3 setup.py build_ext --inplace


EXPOSE 8000/tcp

ENV PATH=/root/.local/bin:$PATH

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD /root/.local/bin/gunicorn -b 0.0.0.0:8000 -w 12 tawhiri.api:app
