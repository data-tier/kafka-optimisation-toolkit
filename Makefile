# kafka-optimisation-toolkit. Run `make help` for the list of targets.

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

KAFKA_TAG ?= 4.3.0
IMAGE     := kot-kafka:$(KAFKA_TAG)
COMPOSE   := docker compose -f cluster/docker-compose.yml
VENV      := .venv
PYTHON    := $(VENV)/bin/python
SWEEP     := bench/sweep.sh

# Optional overrides for every sweep, for example `make sweep-linger RUNS=5`.
# SLOW_RATE and SLOW_DURATION apply to sweep-poll only.
RECORDS     ?=
RECORD_SIZE ?=
RUNS        ?=
SLOW_RATE     ?= 800
SLOW_DURATION ?= 60
SWEEP_OPTS  := $(if $(RECORDS),--records $(RECORDS)) $(if $(RECORD_SIZE),--record-size $(RECORD_SIZE)) $(if $(RUNS),--runs $(RUNS))

export KOT_KAFKA_TAG := $(KAFKA_TAG)

.PHONY: help image cluster-up cluster-down cluster-reset cluster-health \
        sweep-linger sweep-batch sweep-compression sweep-acks sweep-fetch-min \
        sweep-fetchers sweep-poll sweep-all report venv

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

image: ## Build the cluster image (apache/kafka plus JMX exporter and bundled tools)
	@command -v docker >/dev/null || { echo "ERROR: docker is not installed." >&2; exit 1; }
	docker build -t $(IMAGE) -f cluster/Dockerfile --build-arg KAFKA_IMAGE=apache/kafka:$(KAFKA_TAG) .

cluster-up: image ## Start 3 controllers, 3 brokers, the client container and Prometheus
	bash -c 'source bench/lib.sh && require_docker && require_bind_mounts $(IMAGE) && warn_low_resources'
	mkdir -p work
	$(COMPOSE) up -d
	bash -c 'source bench/lib.sh && wait_for_cluster 180'

cluster-down: ## Stop the cluster and keep its data volumes
	$(COMPOSE) down

cluster-reset: ## Stop the cluster and delete all of its data and broker overrides
	$(COMPOSE) down -v --remove-orphans
	rm -f cluster/overrides/broker.properties

cluster-health: ## Print the KRaft quorum status and the under-replicated partition count
	cluster/health.sh

sweep-linger: ## Producer linger.ms 0,5,20,50,100
	$(SWEEP) producer linger.ms 0,5,20,50,100 $(SWEEP_OPTS)

sweep-batch: ## Producer batch.size 16 KiB to 1 MiB with linger.ms=20
	$(SWEEP) producer batch.size 16384,65536,262144,1048576 --fixed linger.ms=20 $(SWEEP_OPTS)

sweep-compression: ## Producer compression.type, once with JSON-like and once with random-byte payloads
	$(SWEEP) producer compression.type none,lz4,snappy,zstd,gzip --payload json --label json $(SWEEP_OPTS)
	$(SWEEP) producer compression.type none,lz4,snappy,zstd,gzip --payload random-bytes --label random-bytes $(SWEEP_OPTS)

sweep-acks: ## Producer acks 1,all
	$(SWEEP) producer acks 1,all $(SWEEP_OPTS)

sweep-fetch-min: ## Consumer fetch.min.bytes 1,65536,1048576
	$(SWEEP) consumer fetch.min.bytes 1,65536,1048576 $(SWEEP_OPTS)

sweep-fetchers: ## Broker num.replica.fetchers 1,2,4 under a full-throttle producer
	$(SWEEP) broker num.replica.fetchers 1,2,4 $(SWEEP_OPTS)

sweep-poll: ## Consumer max.poll.records 100,500,2000 with a 1 ms per record consumer
	$(SWEEP) consumer max.poll.records 100,500,2000 --consumer-app slow --slow-rate $(SLOW_RATE) --slow-duration $(SLOW_DURATION) $(SWEEP_OPTS)

sweep-all: ## Run the seven standard sweeps in order
	$(MAKE) sweep-linger
	$(MAKE) sweep-batch
	$(MAKE) sweep-compression
	$(MAKE) sweep-acks
	$(MAKE) sweep-fetch-min
	$(MAKE) sweep-fetchers
	$(MAKE) sweep-poll

$(PYTHON): requirements.txt
	@command -v python3 >/dev/null || { echo "ERROR: python3 is not installed." >&2; exit 1; }
	python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else "ERROR: Python 3.10 or newer is required.")'
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install --quiet -r requirements.txt
	touch $(PYTHON)

venv: $(PYTHON) ## Create .venv with pandas and matplotlib for the report

report: $(PYTHON) ## Write report.md and chart.png into every results/ directory
	$(PYTHON) bench/report.py results
