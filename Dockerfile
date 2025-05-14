# syntax=docker/dockerfile:1
FROM python:3.13-slim-bookworm

RUN rm -f /etc/apt/apt.conf.d/docker-clean; \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update -y && apt-get upgrade -y \
    && apt-get install --yes --quiet git g++

WORKDIR /accounting-s3-usage
ADD LICENSE requirements.txt ./
ADD accounting_s3_usage ./accounting_s3_usage/
ADD pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip pip3 install -r requirements.txt .

CMD ["python", "-m", "accounting_s3_usage.sampler", "-vv"]
