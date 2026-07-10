.PHONY: openapi client-python client-frontend clients lint-frontend

openapi:
	uv run python scripts/dump_openapi.py

lint-frontend:
	cd frontend && bun run lint

client-python: openapi
	bash client/regenerate.sh

client-frontend: openapi
	cd frontend && bun run generate-client
	$(MAKE) lint-frontend

# Regenerates openapi.json and, only if its content actually changed
# relative to git HEAD, regenerates the Python + frontend clients. Used by
# the local prek hook (see .pre-commit-config.yaml) so most commits (which
# don't change the schema) skip the expensive Docker/bun regeneration steps
# entirely. CI bypasses this gate and calls client-python/client-frontend
# directly for an unconditional full regen (see openapi-clients.yml).
clients: openapi
	@if git diff --quiet -- openapi.json; then \
		exit 0; \
	fi; \
	echo "openapi.json changed — regenerating python + frontend clients..."; \
	$(MAKE) -s client-python client-frontend; \
	echo ""; \
	echo "The OpenAPI schema changed, so the Python and frontend clients were"; \
	echo "regenerated. Review the diff, then:"; \
	echo ""; \
	echo "  git add openapi.json client/src/lclstream_api_client/_generated frontend/src/client"; \
	echo "  git commit"; \
	exit 1
