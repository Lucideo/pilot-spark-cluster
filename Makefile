setup_spark-cluster:
	docker compose --profile spark-cluster up -d --wait

get_jupyter_logs:
	docker compose --profile spark-cluster logs jupyter

down_spark-cluster:
	docker compose --profile spark-cluster down -v

nuke_spark-cluster:
	docker compose --profile spark-cluster down -v \
	&& rm -rf ./data \
	&& rm -rf ./events
