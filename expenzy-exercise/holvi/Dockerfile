FROM python:3.12

RUN adduser --uid 1000 --gecos "" --disabled-password --disabled-login www

# Copy dependency definitions seperately so that any changes to the Python
# source files doesn't invalidate the "pip install" cache layer
COPY --chown=1000 requirements.txt /src/

USER 1000
WORKDIR /src
RUN pip install --user -r requirements.txt \
 && echo 'export PATH=$HOME/.local/bin:$PATH' >> ~/.bashrc
WORKDIR /app
