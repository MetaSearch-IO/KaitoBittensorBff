# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM public.ecr.aws/docker/library/python:3.11-slim

# Allow statements and log messages to immediately appear in the logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

WORKDIR $APP_HOME

# Install production dependencies.
RUN pip install --index-url https://pypi.org/simple --extra-index-url $(cat pip_index_url) --no-cache-dir -r requirements.txt
# RUN pip uninstall -y botocore && pip install botocore==1.31.44
# Install playwright dependencies
#RUN playwright install firefox
#RUN playwright install-deps

ENV PORT 8888
# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 1 --timeout 0 main:app