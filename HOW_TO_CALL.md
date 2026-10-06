# How to Call the API

The primary caller interface is authenticated OAI-PMH 2.0. It returns XML and
implements all six verbs:

- `Identify`
- `ListMetadataFormats`
- `ListSets`
- `GetRecord`
- `ListIdentifiers`
- `ListRecords`

The REST endpoints remain available for provenance and event-cursor
synchronization; those responses are JSON. Defaults allow 200 records per REST
page, 50 per OAI page and eight concurrent connections. There is no per-client
rate limit. Retry temporary 503s/connection errors with exponential backoff.

## Credentials

An operator should send you a private file like:

```dotenv
OJS_API_USERNAME=your-assigned-user
OJS_API_KEY=your-long-random-key
```

Store it outside source control with mode `600`:

```bash
install -m 600 api-client.env "$HOME/.config/ojs-api-client.env"
set -a
. "$HOME/.config/ojs-api-client.env"
set +a
export OJS_BASE_URL=https://api.example.org
```

The key is used as the HTTP Basic password. Use it only over HTTPS:

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/oai?verb=Identify"
```

Missing or invalid credentials return HTTP `401`. `/health` is the only
unauthenticated route and contains no catalogue data.

## XML OAI-PMH calls

All successful OAI responses use `text/xml` and the OAI-PMH 2.0 namespace.
Protocol errors are also OAI XML, normally with HTTP `200`, for example
`badArgument`, `cannotDisseminateFormat`, or `noRecordsMatch`.

### Identify

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/oai?verb=Identify"
```

### List metadata formats

Only `oai_dc` is currently disseminated:

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/oai?verb=ListMetadataFormats"
```

### List ISSN sets

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/oai?verb=ListSets"
```

Sets use `issn:1234567X` form.

### Get one record

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/oai?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:pkp-beacon:article:12345"
```

### List identifiers

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/oai?verb=ListIdentifiers&metadataPrefix=oai_dc"
```

### List records

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/oai?verb=ListRecords&metadataPrefix=oai_dc"
```

Filter by observation date and ISSN set:

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/oai?verb=ListRecords&metadataPrefix=oai_dc&from=2026-07-01&until=2026-07-31&set=issn:1234567X"
```

Dates use `YYYY-MM-DD` and describe when the clean API entity changed, not the
article publication date.

## OAI pagination

`ListSets`, `ListIdentifiers`, and `ListRecords` return an opaque
`resumptionToken` when another page exists. Send it back exactly, with only the
verb:

```text
GET /oai?verb=ListRecords&resumptionToken=TOKEN
```

Do not also send `metadataPrefix`, `from`, `until`, or `set`.

Python harvesting outline:

```python
import os
import xml.etree.ElementTree as ET

import requests

base = os.environ["OJS_BASE_URL"]
auth = (os.environ["OJS_API_USERNAME"], os.environ["OJS_API_KEY"])
params = {"verb": "ListRecords", "metadataPrefix": "oai_dc"}
namespace = {"oai": "http://www.openarchives.org/OAI/2.0/"}

while True:
    response = requests.get(f"{base}/oai", params=params, auth=auth, timeout=120)
    response.raise_for_status()
    root = ET.fromstring(response.content)

    for record in root.findall(".//oai:record", namespace):
        process_record(record)

    token = root.findtext(".//oai:resumptionToken", default="", namespaces=namespace)
    if not token:
        break
    params = {"verb": "ListRecords", "resumptionToken": token}
```

Use retry/backoff for transport failures and HTTP `5xx`. A `401` means the user
or key is wrong and should not be retried indefinitely.

## Deletions and merges

Removed and merged articles are persistent OAI deletion headers:

```xml
<header status="deleted">
  <identifier>oai:pkp-beacon:article:12345</identifier>
  <datestamp>2026-07-01</datestamp>
</header>
```

Delete or tombstone that stable identifier downstream. OAI does not carry a
merge redirect; use the authenticated REST article record when
`merged_into_article_id` is required.

## REST provenance and changes

REST uses the same Basic user/key and returns JSON.

Inspect the current release, event high-water mark, and retained snapshot
history:

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/meta"
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/snapshots"
```

Fetch one canonical article:

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/articles/12345"
```

Fetch its source aliases:

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/articles/12345/sources?after_id=0&limit=200"
```

Page active articles with stable keyset pagination:

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/articles?status=active&after_id=0&limit=200"
```

Use the response’s `next_after_id`; `null` means the scan is complete. Other
statuses are `removed`, `merged`, and `all`. Filters include `changed_since`,
`issn`, `doi`, and up to 100 comma-separated `ids`.

By default an article includes `metadata`, parsed from its source XML into
arrays keyed by metadata field. Bulk callers that only need normalized article
columns can add `structured_metadata=false`. Add
`include_metadata_xml=true` only when the original XML payload is required:

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/articles/12345?structured_metadata=false&include_metadata_xml=true"
```

### Incremental change cursor

Read the release and event high-water mark:

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/meta"
```

Then request events after the last transactionally committed cursor:

```bash
curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
  "$OJS_BASE_URL/changes?after_event_id=123456&limit=200"
```

For each item, use `current_operation`:

```text
upsert  -> insert or update item.article by article_id
delete  -> retain or apply its removed/merged tombstone
```

Apply each page and its final event ID in one downstream database transaction.
Advance the durable cursor only after that transaction commits. Repeating a
page from the old cursor is safe. The durable value is the last returned
`event.event_id`; `next_after_event_id` is `null` on the final page.

The compact deployment keeps the old release available while building and
briefly restarts the API/database at publication. Retry an interrupted request
from its last committed cursor. Always save `high_watermark_event_id` before a
long bootstrap and consume `/changes` from that saved cursor afterward; this
reconciles release changes between pages. Legacy deployments using the online
table-swap publisher can also cross a release between statements in one response.

## Lifecycle fields

Every article has a stable numeric `article_id` and:

| Field | Meaning |
| --- | --- |
| `status` | `active`, `removed`, or `merged` |
| `date_added` | First snapshot observing the entity |
| `date_modified` | Latest API-visible metadata/provenance/status change |
| `date_removed` | First removal observation or merge date |
| `version_number` | Incremented on each API-visible change |
| `merged_into_article_id` | Winner ID for a merged tombstone |
| `data_hash` | Hash of canonical API content |
| `provenance_hash` | Hash of all API-visible source-alias states |

These dates are pipeline observations. Source and publication timestamps are
returned separately.

## HTTP failures

REST validation failures return FastAPI JSON error objects. Handle:

| Status | Meaning |
| --- | --- |
| `401` | Missing or invalid Basic credentials |
| `404` | Requested article ID does not exist |
| `422` | Invalid date, status, ISSN, ID list, or pagination argument |
| `503` | No validated database release is currently available |

Retry connection failures and `5xx` responses with bounded exponential
backoff. Do not retry `401`, `404`, or `422` without changing the request.
