# ---------------------------------------------------------------------------------------------
# Smart Weather Alert Platform (example 24) - ONE image, several components.
# The component a container runs is chosen by its command (see docker-compose.yml / ECS task):
#   ingest_service.py | api_service.py | analytics_worker.py | sensor.py | mcp_weather_server.py
#
# Build from the REPOSITORY ROOT (the context must contain common/ and examples/):
#   docker build -f deploy/capstone/Dockerfile -t weather-platform .
#   docker build --platform linux/amd64 ...   # when building on Apple Silicon for AWS Fargate x86
# Base image is a build arg, e.g. to avoid Docker Hub rate limits on AWS use the ECR Public mirror:
#   docker build --build-arg BASE_IMAGE=public.ecr.aws/docker/library/python:3.12-slim ...
# Docs: https://docs.docker.com/build/building/best-practices/
# ---------------------------------------------------------------------------------------------
ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# 1) dependencies first -> this layer is cached until requirements change
COPY deploy/capstone/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# 2) only the code the platform needs (shared helpers, the gRPC contract of example 12, the capstone)
COPY common/ /app/common/
COPY examples/12_grpc/protos/ /app/examples/12_grpc/protos/
COPY examples/24_capstone_smart_weather_platform/ /app/examples/24_capstone_smart_weather_platform/
COPY deploy/capstone/chaos.py /app/chaos.py

WORKDIR /app/examples/24_capstone_smart_weather_platform

# 3) compile the .proto at BUILD time (not in every container at start-up)
RUN python -c "import proto_stubs"

# 4) run as an unprivileged user; /data holds the commit log + SQLite read model (a volume)
RUN useradd --create-home --uid 10001 app && mkdir -p /data && chown -R app:app /data /app
USER app
VOLUME ["/data"]

EXPOSE 50051 8080

# Default: the public API. Other components override the command.
CMD ["python", "api_service.py", "--data", "/data", "--host", "0.0.0.0", "--port", "8080"]
