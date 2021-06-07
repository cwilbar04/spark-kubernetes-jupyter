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

cluster_standalone_compose:
	docker-compose -f cluster-mode-standalone/docker-compose.yml up

spark_deploy:
	docker build -t spark-base cluster-mode-standalone\spark-base
	docker tag spark-base:latest gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-spark-base:latest
	docker push gcr.io/${GOOGLE_CLOUD_PROJECT}/spark-cluster-spark-base:latest
	
all: install lint test