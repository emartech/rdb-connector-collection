compose=docker-compose -p rdb-connector-collection
default: help

up: ## Start up containers
	$(compose) up -d

ps: ## Status of the containers
	$(compose) ps

stop: ## Stop every container
	$(compose) stop

down: ## Destroy every container
	$(compose) down

mysql: ## Open MySQL console
	$(compose) exec mysql-db mysql -u root -pit-test-root-pw it-test-db

psql: ## Open PostgreSQL console
	$(compose) exec postgresql-db psql -U it-test-user it-test-db

mssql: ## Open SQL Server console
	$(compose) exec mssql-db /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P pwPW123!

help: ## This help message
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' -e 's/:.*#/: #/' | column -t -s '##'

.PHONY: mysql mssql