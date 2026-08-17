PYTHON ?= python3

.PHONY: credentials lightsail-preflight update automatic-update publish-live download scrape scrape-check rebuild-history verify verify-deep test compose

credentials:
	./scripts/generate_api_credentials.sh

lightsail-preflight:
	./scripts/lightsail_preflight.sh

update:
	$(PYTHON) src/run_pipeline.py

automatic-update:
	./scripts/automatic_update.sh

publish-live:
	$(PYTHON) src/publish_live.py

download:
	$(PYTHON) src/run_pipeline.py --download-only

scrape:
	$(PYTHON) src/scrape_updates.py

scrape-check:
	$(PYTHON) src/scrape_updates.py --check

rebuild-history:
	$(PYTHON) src/run_pipeline.py --skip-download --rebuild-history --force-rebuild --resume-building

verify:
	$(PYTHON) src/run_pipeline.py --skip-download --verify-existing

verify-deep:
	$(PYTHON) src/run_pipeline.py --skip-download --verify-source-checksum

test:
	$(PYTHON) -m unittest discover -s tests -v

compose:
	docker compose up --build
