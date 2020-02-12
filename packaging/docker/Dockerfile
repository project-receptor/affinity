FROM python:3 AS builder
COPY LICENSE pyproject.toml poetry.lock /build/
COPY receptor_affinity /build/receptor_affinity
WORKDIR /build
RUN pip install poetry
RUN poetry build -v -f wheel

FROM python:3

COPY --from=builder /build/dist/receptor_affinity*.whl /tmp/
ADD https://github.com/krallin/tini/releases/latest/download/tini /bin/tini
ADD packaging/docker/entrypoint.sh /bin/entrypoint

RUN chmod +x /bin/tini /bin/entrypoint

RUN pip3 install --no-cache-dir /tmp/receptor_affinity*.whl &&\
    rm /tmp/receptor_affinity*.whl

RUN mkdir /var/lib/receptor-affinity
VOLUME /var/lib/receptor-affinity

ENV HOME=/var/lib/receptor-affinity

WORKDIR /var/lib/receptor
ENTRYPOINT ["entrypoint"]
CMD ["affinity", "--help"]
