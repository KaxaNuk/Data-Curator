# app environment: dev, prod
ARG BUILD_ENV=prod

# https://hub.docker.com/_/python
# Cf. https://luis-sena.medium.com/creating-the-perfect-python-dockerfile-51bdec41f1c8
FROM python:3.12-slim AS base

# Setup env
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
# Keep Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1
# Enable Python tracebacks on segfaults
ENV PYTHONFAULTHANDLER=1
# Turn off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

RUN \
  # Install dependencies
  apt-get update \
  && apt-get install -y --no-install-recommends unzip \
  # update pip
  && pip install --upgrade pip \
  # remove unneeded libs
  && apt-get autoremove -y \
  # cleanup
  && rm -rf /var/lib/apt/lists/* \
;

# Path of our app inside the container
RUN mkdir /app
WORKDIR /app


FROM base AS lib_builder
# This stage builds the library, which the other stages will copy from
COPY . .
RUN  \
  pip install pdm \
  && pdm build --no-sdist --dest /kaxanuk \
;


FROM base AS prod_env
# Production environment setup

COPY --from=lib_builder /kaxanuk /usr/local/kaxanuk
RUN \
  LIB_FILE=$(find /usr/local/kaxanuk -name 'kaxanuk_data_curator-*.whl' | head -n 1) \
  && pip install "${LIB_FILE}" \
;


FROM base AS dev_env
# Development environment setup

COPY pyproject.toml ./
COPY README.md ./
COPY src/ ./src/

# Cf. https://github.com/pdm-project/pdm/discussions/2277
RUN \
  pip install pdm \
  # install dependencies
  && pdm install --dev --global --no-lock --project . \
;


FROM ${BUILD_ENV}_env AS final_env
# Final runnable stage, based on the production or development environment based on $BUILD_ENV
ARG BUILD_ENV
# Save BUILDENV to run env variable
ENV KNDC_APPENV=$BUILD_ENV

# execute data_curator on container start
CMD ["kaxanuk.data_curator", "autorun"]
