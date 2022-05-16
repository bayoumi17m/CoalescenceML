from python:3.8.13-slim-buster

WORKDIR /coalescenceml

ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN pip install --no-cache-dir --upgrade --pre pip

RUN apt-get update && \
  apt-get install --no-install-recommends -q -y \
  build-essential \
  ca-certificates \
  curl \
  libsnappy-dev \
  protobuf-compiler \
  libprotobuf-dev \
  unzip && \
  apt-get autoclean && \
  apt-get autoremove --purge

RUN curl -sSL https://install.python-poetry.org | python -
ENV PATH=/root/.local/bin:$PATH
# ENV PATH="$POETRY_HOME/bin:$PATH"

COPY pyproject.toml poetry.lock /coalescenceml/

ENV COML_DEBUG=true

#RUN poetry install --no-root
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction --no-dev --no-root

copy . /coalescenceml

RUN poetry install --no-dev
#RUN poetry update --no-dev && poetry install --no-dev
