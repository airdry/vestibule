# FastAPI sample application.
#
# The build stage installs the pinned dependencies into a prefix that is copied
# into the distroless runtime image. The runtime image ships Python 3.13 and
# glibc but no shell, package manager, or pip, so the application is started
# through the interpreter directly.

# Override with the :debug-nonroot variant to get a BusyBox shell for debugging.
ARG RUNTIME_IMAGE=gcr.io/distroless/python3-debian13:nonroot

FROM python:3.13-slim-trixie AS build

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --prefix=/install -r /tmp/requirements.txt

FROM ${RUNTIME_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# The distroless image resolves site-packages from
# /usr/local/lib/python3.13/dist-packages.
COPY --from=build /install/lib/python3.13/site-packages \
    /usr/local/lib/python3.13/dist-packages
COPY app ./app

USER nonroot:nonroot

EXPOSE 8000

# The distroless image has no shell and no curl, so the probe runs the bundled
# interpreter directly.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD ["/usr/bin/python3", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"]

ENTRYPOINT ["/usr/bin/python3"]
CMD ["-m", "app.main"]
