# lclstream-api-client

A thin sync/async client for the LCLStream API's `/transfers` endpoints, built
on top of an [OpenAPI Generator](https://openapi-generator.tech)-generated
core (`lclstream_api_client._generated`, regenerated via `regenerate.sh`).

## Usage

```python
from lclstream_api_client import LclstreamApiClient

client = LclstreamApiClient(base_url="https://api.example.com", token="SuperSecretToken")

transfers = client.list_transfers()
transfer = client.get_transfer(transfer_id)
```

`token` can also be a zero-arg callable (e.g. for token refresh):

```python
client = LclstreamApiClient(base_url="https://api.example.com", token=get_current_token)
```

An async version is available with the same method names:

```python
from lclstream_api_client import AsyncLclstreamApiClient

client = AsyncLclstreamApiClient(base_url="https://api.example.com", token="SuperSecretToken")
transfers = await client.list_transfers()
await client.aclose()
```

Errors surface as-is from the generated client: non-2xx responses raise
`lclstream_api_client.exceptions.ApiException` (or a typed subclass, e.g.
`UnprocessableEntityException` for 422).

## Regenerating the client

`_generated/` is produced from the live v2 OpenAPI schema and must never be
hand-edited:

```bash
./regenerate.sh
```

See `openapi-generator-config.yaml` for generator options.
