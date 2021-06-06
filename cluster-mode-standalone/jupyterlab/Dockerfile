FROM spark-base

EXPOSE 8888

# Install Google Cloud SDK
RUN apt-get update -y && \
    apt-get install -y python3-pip \
    apt-transport-https \
    ca-certificates gnupg &&\
    apt-get clean

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add -

RUN apt-get update -y && \
    apt-get install google-cloud-sdk kubectl -y && \
    apt-get clean

WORKDIR /tmp
# Install Required Python Packages
COPY requirements.txt requirements.txt
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt && \
    jupyter lab clean 

COPY starter-notebook.ipynb starter-notebook.ipynb

WORKDIR ${SHARED_WORKSPACE}
ENTRYPOINT ["jupyter","lab","--ip=0.0.0.0","--port=8888","--no-browser","--allow-root","--NotebookApp.token="]
# CMD [jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=]