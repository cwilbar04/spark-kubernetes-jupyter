env_variables:
	set GOOGLE_CLOUD_PROJECT=spark-container-test

secret:
	kubectl create secret generic google-credentials \
  --from-file ./sa.json

install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	cd tests
	python -m pytest -vv

lint:
	python -m pylint --disable=R,C tests webapp

venv: 
	python -m venv ..\.venv
	@echo VirtualEnv created. Now run .\..\.venv\Scripts\activate

cluster_standalone_build:
	docker build -t base cluster-mode-standalone\base
	docker build -t jupyterlab cluster-mode-standalone\jupyterlab
	docker build -t spark-base cluster-mode-standalone\spark-base
	docker build -t spark-master cluster-mode-standalone\spark-master
	docker build -t spark-worker cluster-mode-standalone\spark-worker

cluster_standalone_push:
	docker tag base:latest gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-base:latest
	docker push gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-base:latest
	docker tag jupyterlab:latest gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-jupyterlab:latest
	docker push gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-jupyterlab:latest
	docker tag spark-base:latest gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-spark-base:latest
	docker push gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-spark-base:latest
	docker tag spark-master:latest gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-spark-master:latest
	docker push gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-spark-master:latest
	docker tag spark-worker:latest gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-spark-worker:latest
	docker push gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-spark-worker:latest

cluster_standalone_compose:
	docker-compose -f cluster-mode-standalone/docker-compose.yml up

jupyter_deploy:
	docker build -t jupyterlab cluster-mode-standalone\jupyterlab
	docker tag jupyterlab:latest gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-jupyterlab:latest
	docker push gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-jupyterlab:latest
	
all: install lint test