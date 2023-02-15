.PHONY: run_redpandas
run_redpandas:
	docker run -d --name=redpanda --rm \
		-p 8081:8081 \
		-p 8082:8082 \
		-p 9092:9092 \
		-p 9644:9644 \
		-v $(PWD)/data/:/var/lib/redpanda/data/ \
		docker.redpanda.com/vectorized/redpanda:latest \
		redpanda start \
		--overprovisioned \
		--smp 1  \
		--memory 1G \
		--reserve-memory 0M \
		--node-id 0 \
		--check=false

.PHONY: attach_pandas
attach_pandas:
	docker exec -it -u root redpanda bash

.PHONY: stop_redpandas
stop_redpandas:
	docker rm -f redpanda

.PHONY: clean
clean: stop_redpandas
	rm -f db.sqlite3*
	rm -rf data/

.PHONY: run_producer
run_producer:
	cargo run --example producer

.PHONY: run_consumer
run_consumer:
	cargo run --example consumer
