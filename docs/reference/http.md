# Reference: HTTP

Every route, header and error TandemApp serves. The route, header and error tables are written by hand in `tools/http_ref.py` and checked against the code: a path the controller's handlers test, a header name or an error type the code uses that is missing here, or a row the code no longer has, fails `python3 tools/gen_reference.py --check`. The conductor's and the configuration routes come from their modules' route tables. The engine's own API is in [TandemEngine's reference](https://github.com/TandemLLM/TandemEngine/blob/main/docs/reference/http.md).

Every route but `/health`, the page and the sign-in needs an account's session or an API token; [auth.md](../auth.md) has the roles, the scopes and the recovery steps. The first table names the labels of the routes' `who` column.

<!-- generated: http-who by tools/gen_reference.py; do not edit by hand -->

### Who may call a controller route: the permission proxy/authz.py names, and the lowest role that holds it

| permission | held by | what it covers |
|-|-|-|
| `open` | anyone | anyone who reaches the port: the page shell, `/health`'s status, the sign-in and the first-run setup (which checks its own setup code) |
| `self` | viewer and up | a signed-in person's own sessions, password, API tokens and usage |
| `users` | admin and up | every account but the owners, every session, the audit |
| `session-address` | the session address | a `/s/<token>/` address: its token is the credential, for the library and Laya only |
| `collect:write` | a service identity | a service identity only: a backend's collector or plugin (svc:collect:&lt;id&gt;) |
| `plugin-backend` | a service identity | a service identity only: the plugin relay, in process |
| `read` | viewer and up | every read of the page. For the fleet, the settings, the configuration and the conductor a write needs its kind's own permission too: settings:write, fleet:write or trust |
| `chat-engine` | a service identity | a service identity only: chatd (svc:chatd) |
| `chat` | operator and up | the Chat tab |
| `metrics:read` | viewer and up | the Prometheus page |
| `inference` | viewer and up | the data plane: the OpenAI and Messages API routes, the models, the library and Laya; an API token needs the protocol's scope (inference:openai, inference:anthropic) |

### Who may call a route the extension serves inside an engine: the engine's own rule

| label | who may call |
|-|-|
| `open` | anyone who reaches the port |
| `admin` | the admin rule: anyone while no admin password is set; with one, the admin token, a dashboard session, or a tool on the machine itself |
| `session` | only under a session address, `/s/<token>/...` |

<!-- /generated -->

<!-- generated: http-proxy-routes by tools/gen_reference.py; do not edit by hand -->

### The controller's routes (`tandem-app`)

| route | permission | what |
|-|-|-|
| `POST` `/v1/chat/completions` | inference | Relayed to the backend the model id routes to ([routing by model id](../backends.md#routing-by-model-id)); the answer's bytes go back unchanged. The same for `/v1/completions`, `/v1/embeddings` and `/v1/responses.` *Body:* the client's body. *Answer:* the backend's answer, with X-Tandem-Backend. |
| `POST` `/v1/completions` | inference | As `/v1/chat/completions.` |
| `POST` `/v1/embeddings` | inference | As `/v1/chat/completions`; not for deepseek backends. |
| `POST` `/v1/responses` | inference | As `/v1/chat/completions`; openai backends only. |
| `GET` `/v1/models` | inference | Every backend's models under their real ids, `owned_by` the backend. *Answer:* `{object: list, data: [... with a tandem block]}`. |
| `GET` `/v1/models/{id}` | inference | One model's card. *Answer:* the card, or 404 `model_not_found`. |
| `GET` `/` | open | Redirects to the dashboard. *Answer:* 302 to `/dashboard/`. |
| `GET` `/dashboard/` | open | The dashboard's static files. *Answer:* the page. |
| `GET` `/v1/dashboard/{view}` | read | summary, usage, requests, system, live, logs, metrics, parallel and session, as on the engine; the engine-only panels answer 404. *Answer:* as on the engine. |
| `POST` `/v1/dashboard/session` | open | As on the engine. *Body:* `{token}`. *Answer:* the session. |
| `*` `/v1/config/*` | read | The Backends and Setup screens ([the configuration routes](#the-configuration-routes-v1config)). |
| `GET` `/v1/fleet/*` | read | TandemApp's fleet ([the fleet](../proxy.md#the-fleet-v1fleet)): nodes, links, topology, deployments, per-node metrics, memory, logs, events, alerts, rules and a page's window layout, as app/contracts/fleet-v1 says. *Answer:* the contract's JSON. |
| `*` `/v1/fleet/*` | read | The fleet's writes: PATCH `/v1/fleet/nodes/{id}` (alias, colour, icon, tags), PUT `/v1/fleet/layouts/{page}`, POST `/v1/fleet/alerts/{id}/ack.` *Body:* the contract's body. *Answer:* the node, the layout as saved, or the alert. |
| `POST` `/v1/collect/{backend}` | collect:write | A plugin's batch (as on the engine). |
| `GET` `/metrics` | metrics:read | The proxy's own Prometheus families. *Answer:* Prometheus text format. |
| `GET` `/health` | open | The proxy's health, for a container's check; the full body for the admin or the machine. Also `/healthz` and `/v1/health.` *Answer:* `{status}`. |
| `*` `/v1/conductor/*` | read | The conductor in front of the backends, with `--conductor-config`; the engine-only screens answer 404. |
| `GET` `/v1/library` | inference | The package library, as on the engine, also under `/v1/library/*.` |
| `GET` `/v1/laya` | inference | Laya, when the decider role names an endpoint. |
| `POST` `/v1/laya/ask` | inference | As on the engine, relayed to the decider role's endpoint. |
| `POST` `/v1/messages` | inference | The Messages API: translated to a chat completion and run through the pipeline, or passed through untouched when every backend of the model's route is of kind anthropic (docs/proxy.md). *Body:* the Messages API's body, the key as x-api-key or a bearer. *Answer:* the Messages API's answer or event stream, with request-id. |
| `POST` `/v1/messages/count_tokens` | inference | The prompt's exact token count from a backend that can count it; any other answers 404 `not_found_error`. *Answer:* `{input_tokens}`. |
| `POST` `/v1/cache/peek` | inference | With a deployment's model: relayed to the member that holds the conversation; the same for `/v1/cache/prewarm` and `/v1/cache/release.` Without the model: 400 `model_required`. *Body:* `{model, session?, ...}`. *Answer:* the member's answer, with X-Tandem-Node and X-Tandem-Deployment. |
| `POST` `/v1/cache/prewarm` | inference | As `/v1/cache/peek.` |
| `POST` `/v1/cache/release` | inference | As `/v1/cache/peek.` |
| `GET\|POST\|DELETE` `/v1/auth/session` | open | Who this request is (account, role, permissions, CSRF token, provider, `setup_required`, `local_mode`); POST signs in, DELETE signs out (docs/auth.md). *Body:* POST: `{username, password}`. *Answer:* the session; the cookie on a sign-in. |
| `POST` `/v1/auth/setup` | open | The first owner, with the one-time setup code; 404 once an owner exists. *Body:* `{code, username, password}`. *Answer:* the session. |
| `PUT` `/v1/auth/password` | self | A signed-in person's own password. *Body:* `{current, password}`. |
| `GET\|DELETE` `/v1/auth/sessions` | self | The live sessions (?user= another's, with the users permission); DELETE ends them, DELETE ...`/{id}` one. |
| `GET\|POST\|PATCH\|DELETE` `/v1/auth/tokens` | self | API tokens (?all=1 every account's): POST makes one, PATCH `/{id}` changes one, DELETE `/{id}` revokes, POST `/{id}/rotate`, GET `/{id}/usage.` *Answer:* a new token's secret once. |
| `GET\|POST\|PATCH\|DELETE` `/v1/auth/users` | users | The accounts: POST makes one, PATCH `/{id}` changes one, DELETE `/{id}` removes one, POST `/{id}/reset` a one-time code. |
| `GET` `/v1/auth/audit` | users | Sign-ins, sessions, accounts, tokens and the auth settings, as audit rows. |
| `GET` `/v1/auth/usage` | self | What an account used (?user= another's, with the users permission). |
| `GET` `/v1/chat-app/capabilities` | read | What the Chat tab can do here: whether chatd answers, the sandbox, the session. |
| `*` `/v1/chat-app/*` | chat | The Chat tab: relayed to chatd behind the controller (off, own or external; docs/chat.md). *Answer:* chatd's answer. |
| `GET\|POST` `/s/{token}/v1/library/*` | session-address | The library and Laya under a session address, the address the notes give the model, journaled to that session. |
| `*` `/v1/settings/*` | read | The settings interface over every store: scopes, change sets, versions and restore, the audit (docs/settings.md). Reads need the read permission, writes the scope's own. *Body:* the contract's body. *Answer:* the contract's JSON. |
| `GET` `/plugin-panels/{id}/{panel}/` | read | A running plugin's panel: its HTML and the files beside it, served sandboxed for the page's frame (docs/plugins.md). *Answer:* the panel's files. |
| `*` `/plugin-backends/{id}/{name}` | plugin-backend | A plugin's backend, the URL of its row in the backends store; the gateway routes a model there. From this machine only (403 forbidden otherwise). *Body:* the relayed request. *Answer:* the plugin's answer. |

### The host collector beside the controller

| server | route | answer |
|-|-|-|
| `tandem-app hostcollect` | `GET` `/host?watch=` | the machine's sample as JSON (the bearer token when `--token` is set) |
| `tandem-app hostcollect` | `GET` `/metrics` | the same sample in the Prometheus text format |
| `tandem-app hostcollect` | `GET` `/health` | `{status: ok}`, no token |

<!-- /generated -->

<!-- generated: http-ext-routes by tools/gen_reference.py; do not edit by hand -->

### The routes the extension serves inside an engine

| route | who | what |
|-|-|-|
| `*` `/v1/conductor/*` | admin | The conductor's documents, journal, sessions and memory ([the conductor's routes](#the-conductors-routes-v1conductor)). Reads follow the dashboard rule, writes and every memory route the admin rule. On a headless engine, or one without `--conductor-config`, 404. |
| `GET` `/v1/laya` | open | What the Laya route takes and answers, its limits, and whether it is on. *Answer:* a description of the route. |
| `POST` `/v1/laya/ask` | open | A typed answer from Laya on one text or many, for the model's own scripts; rate-limited. *Body:* `{question, choices \| scale \| neither, text \| texts, threshold?, model?, deadline_ms?}`. *Answer:* `{label, p, sure, probs, model, ms, ...}`, or `{answers: [...]}` for texts. |
| `POST` `/v1/laya/drive/end` | open | Ends a playtest driver session and writes its journal row. *Body:* the session's summary. *Answer:* the row. |
| `GET` `/v1/laya/kit/{file}` | open | A file of the playtest kit the model copies into a project. *Answer:* the file. |
| `OPTIONS` `/v1/laya/*` | open | A browser's CORS preflight for the Laya routes. Every other path answers OPTIONS with 501. *Answer:* 204 with the CORS headers. |
| `GET` `/v1/library` | open | The package library: every enabled skill's name, description and classes. *Answer:* the list and the two routes below. |
| `GET` `/v1/library/search?q=&k=&class=` | open | BM25 search over the skills, best first (k from 1 to 20, default 5). *Answer:* [`{name, description, score}`]. |
| `GET` `/v1/library/{name}` | open | One skill's text and its reference. *Answer:* `{name, description, classes, body, reference}`. |
| `GET` `/s/{token}/v1/memory` | session | The memory index this session sees. The `/s/{token}/` prefix also serves the library and Laya routes, filed to the session. *Answer:* the notes' index lines. |
| `GET` `/s/{token}/v1/memory/search?q=&k=` | session | Memory notes that match, best first (k from 1 to 10). *Answer:* [`{id, kind, scope, status, title, score}`]. |
| `GET` `/s/{token}/v1/memory/{id}` | session | One note. *Answer:* `{id, title, kind, scope, status, body, ...}`. |
| `POST` `/s/{token}/v1/memory` | session | Writes, changes or archives a note, with the fields of the hidden memory block. *Body:* `{op, scope, kind, title, tags, body, id, reason}`. *Answer:* `{op, id, title, ...}`, or 422 with the reason as the type. |

<!-- /generated -->

<!-- generated: routes by tools/gen_reference.py; do not edit by hand -->

### The conductor's routes, `/v1/conductor/*`

| Method | Path | What |
|-|-|-|
| `GET` | `/v1/conductor/config` | loaded versions, load errors, journal, roles |
| `GET` | `/v1/conductor/meta` | enums, the task catalogue, the schemas |
| `GET` | `/v1/conductor/profiles` | tiles |
| `GET` | `/v1/conductor/{kind}[/{name}]` | settings, router, roles, packages; profiles/{name} |
| `PUT` | `/v1/conductor/{kind}[/{name}]` | `{doc, note, base_version?}`: a new version |
| `GET` | `.../versions .../versions/{v} .../diff?from=&to=` | a document's versions, one version, and the diff between two |
| `POST` | `.../restore` | `{version, note?}` a new version equal to an old one |
| `POST` | `/v1/conductor/profiles/{name}/mode` | `{mode, note?}`: the three-way switch |
| `GET` | `/v1/conductor/roles` | the document, health, and what each role's tokenizer hash allows it to share |
| `POST` | `/v1/conductor/roles/{role}/test` | one question, its latency and answer |
| `GET` | `/v1/conductor/packages/scan` | skills found in skill\_dirs on the box |
| `GET` | `/v1/conductor/packages/catalog` | every package: category, tokens on the engine's tokenizer, limit, classes, library switches |
| `GET` | `/v1/conductor/packages/item/{name}` | one package's texts, tokens, BM25 neighbours |
| `POST` | `/v1/conductor/packages/tokens` | `{texts}` the tokens of up to 20 texts (a read) |
| `GET` | `/v1/conductor/packages/usage?name=&limit=` | injections, shadow notes and library picks per package in the last day and week (journal) |
| `POST` | `/v1/conductor/classify` | `{text, profile?, tools?}`: the test box |
| `GET` | `/v1/conductor/decisions[/{id}]` | the live feed (router rows, each with the monitor's `events` of its request) and one row |
| `POST` | `/v1/conductor/decisions/{id}/label` | `{label: right\|wrong\|clear, note?}` (any row) |
| `GET` | `/v1/conductor/events?task=&mode=&acted=&session=&request=&since=&limit=` | the session monitor's decisions |
| `GET` | `/v1/conductor/events/{id}` | one of them |
| `GET` | `/v1/conductor/stats?hours=24` | the Overview's tiles and ribbon, `monitor` too |
| `GET` | `/v1/conductor/sessions[/{key}[/export?format=json\|md]]` | sessions, one timeline, the honest record: every hidden text of router and monitor alike |
| `GET` | `/v1/conductor/evals` | classifier and suite reports on the box |
| `POST` | `/v1/conductor/evals` | `{report}`: a suite report (tools/tandem\_eval.py) |
| `GET` | `/v1/conductor/residency/plan` | the residency planner: limits, what fits now, the engine's split, the last refusal |
| `GET` | `/v1/conductor/watch` | the session tasks' state per session |
| `GET` | `/v1/conductor/deciders/status` | the decision models this engine serves: states, limits, numbers, the planner's answer for a load now, the model behind each question |
| `POST` | `/v1/conductor/deciders/models/{name}/load\|unload` | through the residency planner (write) |
| `POST` | `/v1/conductor/deciders/autoload` | `{service_start?, hold_restart?}`: the on\_start models, one after the other (ops/start.sh; write) |
| `POST` | `/v1/conductor/deciders/test` | `{state, questions, model?}`: the test box |
| `GET\|PUT` | `/v1/conductor/deciders` | the `deciders` document, like the others |
| `GET` | `/v1/conductor/narrator/status` | the narrator's slot: its model, state, contract, calls and latency |
| `POST` | `/v1/conductor/narrator/try` | `{events: [{kind, facts, extra?}`], budget\_ms?, max\_tokens?}: each event's template and the narrator's line, in the idle window (write) |
| `POST` | `/v1/conductor/narrator/load\|unload` | through the residency planner (write) |
| `GET` | `/v1/conductor/shadow/status` | the shadow reader (SR1): its slot, the sessions' states (read, behind, bytes), the router's agreement, the last answers |
| `POST` | `/v1/conductor/shadow/ask` | `{session, question, choices: {id: text}`, budget\_ms?}: a typed decision of the small model over the session's whole context, in the idle window (write) |
| `GET` | `/v1/conductor/memory/sessions` | the session transcripts on the box: one row per session, the writer's counters and settings |
| `GET` | `/v1/conductor/memory/sessions/{key}?page=N` | its blocks, 200 a page (default: the newest page), to the last complete block |
| `GET` | `.../memory/sessions/{key}/export?format=md\|json` | the transcript itself |
| `PATCH` | `.../memory/sessions/{key}/keep` | `{keep}`: meta.json's keep, which the prune honours |
| `GET` | `/v1/conductor/memory/disk` | the disk tier: running or not, its settings and their version, bytes, budget, free space, hit rate, the most-hit entries (a preview for opencode heads and packages ... |
| `POST` | `/v1/conductor/memory/disk/clear` | every entry and count gone (write) |
| `GET` | `/v1/conductor/memory/notes?user=` | the memory notes: the users, the tree (scopes with counts), every live note's frontmatter with its uses in 7 days, the archived ones, the duplicates, the store's counts |
| `GET` | `.../memory/notes/search?user=&q=&k=` | the runtime's search over every scope, with the scores |
| `GET` | `.../memory/notes/injected?user=&limit=` | the last requests' injected note ids (journal) |
| `GET` | `.../memory/notes/writes?limit=` | the last writes and refusals (journal) |
| `GET` | `.../memory/notes/export?user=` | a zip of the user's directory |
| `GET` | `.../memory/notes/{id}?user=` | one note: frontmatter, body, changelog, the revisions, its uses by day |
| `PUT` | `.../memory/notes/{id}?user=` | `{title, body, kind, tags, verify_by}`: a person's edit (a revision; ratified, writer human) |
| `PATCH` | `.../memory/notes/{id}?user=` | `{status?, pinned?}`: ratify, back to provisional, pin a ratified rule |
| `POST` | `.../memory/notes/{id}/archive?user=` | `{reason}` |
| `DELETE` | `.../memory/notes/{id}?user=` | the note, its revisions and archive copy, at once |
| `DELETE` | `.../memory/notes/users/{user}` | `{confirm: <user>}`: the whole directory |

### The configuration routes, `/v1/config/*`

| Method | Path | What |
|-|-|-|
| `GET` | `/v1/config` | everything the screens show, in one answer |
| `POST` | `/v1/config/test` | `{kind?, url, key?}`: test a backend not saved; the answer says `reason` (key\_rejected, unreachable, not\_openai) when it fails |
| `POST` | `/v1/config/backends` | `{url, id?, kind?, key?,...}`: add. `kind` defaults to generic (any OpenAI-compatible server), `id` to a free one made from the label or the URL's host. |
| `GET` | `/v1/config/presets` | the providers the page offers (proxy/presets.py) |
| `GET` | `/v1/config/backends/{id}` | one, with its models |
| `PATCH` | `/v1/config/backends/{id}` | change fields (`key: null` removes the key) |
| `DELETE` | `/v1/config/backends/{id}` |   |
| `POST` | `/v1/config/backends/{id}/test` | test the saved backend, refresh its models |
| `PUT` | `/v1/config/backends/{id}/models/{model}` | public id, enabled, price, max\_output (null clears), fallback, features (creates a manual model) |
| `DELETE` | `/v1/config/backends/{id}/models/{model}` |   |
| `PUT` | `/v1/config/rules` | `{rules: [{id?, pattern, backend, enabled}`]} |
| `GET` | `/v1/config/prices/seed` | the public list prices shipped with this release, applied only by a click |
| `GET` | `/v1/config/audit` | the last 50 changes (field names only) |

<!-- /generated -->

<!-- generated: http-headers by tools/gen_reference.py; do not edit by hand -->

### Every header TandemApp reads or writes

| header | direction, where | meaning |
|-|-|-|
| `x-api-key` | in, controller | a Messages API client's credential, as an API token or a backend key would be sent as a bearer; removed from the request once checked |
| `X-Tandem-Auth` | out, controller | accounts on `/health`: the controller runs the accounts sign-in, so a front proxy asks it |
| `X-Tandem-CSRF` | in, controller | a cookie request that writes carries the session's token (GET `/v1/auth/session` returns it); without it 403 |
| `X-Tandem-Dropped` | out, controller | the client's fields a translated request went without (a Messages API field the chat shape has no place for, thinking the stash evicted) |
| `X-Tandem-Hook` | both, controller | to a webhook: the hook's id; on an answer a guard refused: the hook that refused it |
| `X-Tandem-Delivery` | out, controller | to a webhook: the call's id, also the body's id, which the signature covers |
| `X-Tandem-Timestamp` | out, controller | to a webhook: unix seconds, inside the signed text |
| `X-Tandem-Signature` | out, controller | to a webhook: v1=&lt;hex HMAC-SHA256(secret, timestamp.body)&gt;, one v1= per secret while a secret rotates (docs/hooks.md) |
| `X-Tandem-*` | out, controller | every X-Tandem- answer header, and the backend's anthropic-ratelimit-\* headers, also reach a Messages API client |
| `X-Tandem` | both, both | raw, shadow or on: lowers the conductor's mode for one request; the response says which mode ran |
| `X-Tandem-Profile` | both, both | picks a conductor profile; the response names the profile that ran; an unknown name is a 400 |
| `X-Tandem-Memory` | in, engine | off: no memory note is read or written; read: reads only, a block the model writes is hidden and dropped |
| `X-Tandem-User` | in, engine | a proxy's user name for the memory notes, honoured from loopback or QWEN38\_MEMORY\_PROXIES |
| `X-Tandem-Relay-Id` | in, engine | a proxy's id for a relayed request, for the live counts; a relayed request without X-Tandem-User touches no memory |
| `X-Tandem-Parent` | in, engine | the request a call of TandemLLM to itself serves; the row becomes client kind internal |
| `X-Tandem-Forward` | out, engine | to chatd on a `/v1/chat-app` request: the forward key, the only proof chatd accepts |
| `X-Tandem-Principal` | out, engine | to chatd: who the request is for (owner) |
| `X-Tandem-Password-Set` | out, engine | to chatd: 1 when an admin password is set |
| `X-Tandem-Signed-In` | out, engine | to chatd: 1 when the request passed the dashboard's rule |
| `X-Tandem-Backend` | out, proxy | the id of the backend that answered a relayed request |
| `X-Tandem-Deployment` | out, proxy | the deployment that answered a request the gateway routed to one of its members |
| `X-Tandem-Node` | out, proxy | the alias of the node whose engine answered a request the gateway routed |
| `X-Tandem-Pin` | in, proxy | a node's id or alias: the request goes to that member of the deployment and to no other (the Playground's A/B of a deployment's members) |
| `X-Tandem-Gateway` | out, proxy | tandem-app on the controller's `/v1/models` answers, so a page behind a reverse proxy knows whether its `/v1` reaches the controller or an engine |
| `X-Tandem-Bundle` | out, both | on the page's files: the bundle's tag, or none while no bundle is built and the placeholder answers |
| `X-Tandem-Copy-Token` | in, proxy | TandemNode: the token of one weights copy between two nodes; a file sent without it is refused |
| `X-Tandem-Cost` | in, proxy | 1 on a streamed relayed request: one SSE comment before data: `[DONE]` carries the request's cost (: tandem-cost `{"cost_micros": N, "backend": id}`); without it the stream is the backend's |
| `x-session-id` | in, both | opencode's session id: the router's session and the caches' session key |
| `X-Conversation-Id` | in, both | a conversation id for clients that send no session field; read after the body's own ids |
| `X-OpenWebUI-Chat-Id` | in, both | Open WebUI's chat id (a connection header with `{{CHAT_ID}}`): the router's session for that chat |
| `X-Request-Id` | both, proxy | sent to a vllm backend as px-&lt;hex&gt;, which the plugin reports back for the join; a client's own id is kept |
| `X-QSE-Client` | in, engine | dashboard marks the dashboard's own requests (client kind dashboard) |
| `X-Real-IP` | in, both | set by a reverse proxy; a request that carries it is never trusted as local; the sign-in's rate limit counts per address |
| `X-Forwarded-For` | in, both | as X-Real-IP: a request that carries it is never trusted as local |
| `X-Forwarded-Proto` | in, both | https makes the session cookie Secure, and the notes' `{tandem_url}` https |
| `X-Forwarded-Host` | in, engine | the host the notes' `{tandem_url}` names, unless QWEN38\_TANDEM\_URL sets it |
| `X-Accel-Buffering` | out, both | no, on every stream, so nginx sends each event at once |
| `X-Content-Type-Options` | out, both | nosniff on the dashboard's static files |
| `x-ratelimit-*` | out, proxy | a cloud backend's rate-limit headers, passed on to the client |
| `OpenAI-Beta` | both, proxy | a client's OpenAI-Beta header, passed on to an openai backend |
| `Authorization` | in, both | Bearer: an API token (tdm-...), a service identity's token (a collector, chatd) or a management token for scripts, by route (docs/auth.md); the engine's OpenAI routes ignore it and file the request under a hash of it |
| `User-Agent` | in, both | the client kind in the ledger: open-webui, opencode, openai-sdk, curl, other (a Hermes-shaped body: hermes) |
| `Last-Event-ID` | in, both | resumes the log stream after that line |
| `Retry-After` | out, both | with a 503 (queue full, memory gate, draining, a full backend), a 429, or a decision model that is loading |
| `WWW-Authenticate` | out, both | Bearer, with a 401 |
| `Set-Cookie` | out, both | `qse_dash`, the dashboard session: HttpOnly, SameSite=Strict, Secure behind https |
| `Access-Control-Allow-Origin` | out, engine | \* on the library and Laya routes, so a page the model builds can call them; with Access-Control-Allow-Methods, -Headers and -Max-Age on the preflight |
| `Access-Control-Allow-Methods` | out, engine | on the Laya preflight |
| `Access-Control-Allow-Headers` | out, engine | on the Laya preflight |
| `Access-Control-Max-Age` | out, engine | on the Laya preflight |

<!-- /generated -->

<!-- generated: http-errors by tools/gen_reference.py; do not edit by hand -->

### The errors explained

| status | error.type or code | when |
|-|-|-|
| `400` | `invalid_request_error` | a request TandemApp will not relay as asked: error.param names the field |
| `400` | `bad_request` | a dashboard, fleet or conductor query or body that does not parse (a level, a range, a number, a body that is not an object) |
| `400` | `model_required` | a cache route through the gateway without the model that names the deployment |
| `400` | `context_length_exceeded` | a prompt and `max_tokens` that no member of the deployment has the window for |
| `401` | `unauthorized` | a route that needs a credential, without a session or an API token, or with one that is unknown, revoked or expired; a wrong collect token; `wrong_credentials`: a sign-in whose name or password is wrong |
| `401` | `invalid_api_key` | the data plane (OpenAI shape): no API token, a token it does not know, or two credentials that name different tokens (code `invalid_api_key`) |
| `403` | `forbidden` | a memory route outside a known session address; a cross-site write; a weights copy with another token |
| `403` | `wrong_password` | PUT `/v1/auth/password` whose current password is not right |
| `404` | `not_found` | no such route, or a route whose part is off on this server (the conductor, the backends, the fleet) |
| `404` | `unsupported_route` | the proxy: a route the backend's kind does not serve, such as embeddings on deepseek (code `unsupported_route`) |
| `404` | `model_not_found` | a model id no backend serves, or one two backends serve (the message names the prefixed forms) (code `model_not_found`) |
| `405` | `method_not_allowed` | the sign-in routes, the accounts or the Setup Guide's relay with a method they do not take |
| `409` | `busy` | an Add Spark session or an install that already runs; a chatd job that runs on TandemNode |
| `409` | `closed`, `stale` | a package proposal already closed, or whose base text changed since |
| `409` | `conflict` | an Add Spark session in another state, a device picked twice, more devices than one add takes |
| `409` | `not_a_member` | X-Tandem-Pin names a node that is not a member of the deployment |
| `413` | `too_large` | a request body over the limit (a fleet request's body over 4 MiB; a chunked body is measured against its route's cap as it arrives, not only by its Content-Length) |
| `422` | `invalid_request_error`, `invalid_report` | a value the schema refuses: a config field (error.field), a field an environment variable locks (error.variable), a Laya, library or memory call's fields, an eval report |
| `422` | `schema`, `secret`, `cap`, `user`, `unknown_id`, `unknown_package`, `rule_from_tool`, `no_session`, `no_user`, `refused_root`, `no_proposals`, `propose` | a memory note or a package proposal refused, with the reason as the type: a field (schema), a secret, a cap, an unknown note or package |
| `429` with `Retry-After` | `too_many` | a fifth log or live stream at once; a sixth failed sign-in in a minute; too many wrong passwords or pairing attempts |
| `429` with `Retry-After` | `rate_limited` | the library, Laya or memory routes past their calls a minute |
| `500` | `internal_error`, `error` | a bug, or a fleet job that failed on an exception: the message is the exception's |
| `502` | `connect_error`, `upstream_error`, `key_unreadable` | the proxy could not reach the backend or read its stored key, after its retries |
| `502` | `unreachable` | Add Spark: a paired device whose agent does not answer, or answers with an error |
| `200` | `upstream_closed`, `upstream_error` | a stream that already began: the backend failed or closed early; the client gets an error event, then `[DONE]` |
| `503` with `Retry-After` | `server_busy` | a backend or a deployment that cannot take the request now; error.code says why (the codes below) |
| `503` | `server_busy` | the bodies being read at this moment are past the box's budget (2 GiB, `QSE_MAX_INFLIGHT_BODY_MB`): the request is refused unread |
| `503` with `Retry-After` | `max_inflight` | every backend for the model is at the `max_inflight` you set (Retry-After: 1) |
| `503` with `Retry-After` | `deployment_stopped` | a request for a deployment that is stopped, or a Split without a plan |
| `503` with `Retry-After` | `no_member`, `member_not_serving` | a deployment with no member that takes requests now, or the pinned member does not |
| `503` with `Retry-After` | `node_draining` | a member whose node is drained for maintenance |
| `503` with `Retry-After` | `not_loaded` | a Laya call while the engine has handed over no decision model |
| `504` | `upstream_timeout`, `timeout` | a backend that sent no first byte in time; a Laya call past its deadline |

### Every other error type the code answers, with its status and the code's own message ({...} is a value)

| error.type or code | status | message | where |
|-|-|-|-|
| `address_in_use` | 409 | the pairing window cannot listen there: {...} | `proxy/nodeagent.py` |
| `api_error` | in the body |   | `proxy/relay.py`, `proxy/service.py` |
| `bad_arguments` | 400 | {...}: {...} | `proxy/pluginhost.py` |
| `bad_password` | 400 |   | `proxy/accounts.py` |
| `bad_value` | 422 | {...} has no unset: set a value | `proxy/hooksettings.py`, `proxy/pluginsettings.py` and 4 more |
| `bad_wheel` | 422 | not a wheel (a zip archive) | `proxy/pluginapi.py` |
| `chat_config` | 409 | chatd's env file: {...} | `proxy/app.py` |
| `chat_off` | 409, 503 | Chat is off on this TandemApp: switch it on in Settings, App, What runs here | `proxy/appchat.py`, `tandemapp/chat_forward.py` |
| `chat_unavailable` | 502 | chatd has not started on this box yet (no forward key) | `tandemapp/chat_forward.py` |
| `confirm_invalid` | 409 | that confirm is spent, too old or another file's: read the plan again | `proxy/appsettings.py`, `proxy/ctlmove.py` and 3 more |
| `confirm_needed` | 409 | the full payload (prompts and answers as they are) would go to {...}, outside the LAN: send the same request again with this confirm to grant it | `proxy/hookstore.py`, `proxy/pluginstore.py` and 1 more |
| `confirm_required` | 428 | an import needs its plan's confirm: POST /v1/fleet/app/import first and send its `confirm` | `proxy/appsettings.py`, `proxy/ctlmove.py` and 5 more |
| `content_mismatch` | 502 | {...}'s copy has the content id {...}, not {...} | `proxy/fleetweights.py` |
| `disabled` | 409 | {...} is disabled: enable it first | `proxy/pluginhost.py`, `proxy/pluginstore.py` |
| `downgrade` | 409 | {...} is older than the installed {...}: install with allow\_downgrade true to go back | `proxy/pluginstore.py` |
| `drain_timeout` | 504 | {...} requests still in flight after {...} s | `proxy/deployments.py`, `proxy/fleetupdate.py` |
| `ended` | 409 | restart {...} is {...} already | `proxy/settingsengine.py` |
| `engine_busy` | 409 |   | `proxy/settingsengine.py` |
| `engine_down` | 503 | the engine on :{...} does not answer | `proxy/nodeagent.py` |
| `engine_failed` | 502 |   | `proxy/settingsengine.py` |
| `engine_refused` | 409 |   | `proxy/settingsengine.py` |
| `engine_timeout` | 504 |   | `proxy/settingsengine.py` |
| `engine_too_old` | 502 |   | `proxy/settingsengine.py` |
| `engine_unreachable` | 502 | PATCH /v1/engine/settings: {...} | `proxy/app.py`, `proxy/settingsengine.py` |
| `exists` | 409 | a hook {...} exists | `proxy/hookstore.py`, `proxy/nodeweights.py` and 2 more |
| `extension_missing` | 409 | the engine host has no extension {...} that loads, and TandemApp installs only its own ({...}\*): install {...} there first (tandem remote ext install), or choose {...} | `proxy/settingsengine.py` |
| `external_account` | 409 | this account signs in elsewhere: it has no password here | `proxy/authapi.py` |
| `failed` | 500 | the start command exited {...}; its output: {...}{...} | `proxy/nodeengines.py` |
| `fingerprint_confirm_required` | 428 | {...} feed an engine's disk-cache fingerprint: every saved prefix misses after its restart; send the plan's confirm\_fingerprint as well | `proxy/appsettings.py`, `proxy/plugintrust.py` and 1 more |
| `fingerprint_mismatch` | 422 | confirm\_fingerprint is not this key's fingerprint: do not trust a key whose fingerprint differs from its publisher's | `proxy/plugintrust.py` |
| `gone` | 409 | node {...} left the fleet during the update | `proxy/config_api.py`, `proxy/fleetupdate.py` |
| `grant_confirm_required` | 428 | {...}: prompts and answers would go to a destination outside the LAN; send the plan's confirm\_grant as well | `proxy/appsettings.py`, `proxy/settings.py` |
| `hash_mismatch` | 422, 502 | {...}: sha256 {...}… is not the manifest's {...}… | `proxy/nodeweights.py` |
| `host_key_changed` | 409 | the host key of {...} changed; refused | `proxy/addspark.py`, `proxy/settingsengine.py` |
| `host_key_unconfirmed` | 409 | confirm the host key first (read it with .../hostkey and compare it on the node) | `proxy/addspark.py` |
| `identity_mismatch` | 502 | the stages do not fit each other: {...} | `proxy/splitrun.py` |
| `incomplete` | 409 | {...} files have not arrived (the first: {...}) | `proxy/nodeweights.py` |
| `install_failed` | 500, 502 | {...}: {...} | `proxy/fleetupdate.py`, `proxy/nodeupdate.py` |
| `integrity` | 409 | {...}: its tree is {...} since the install ({...}); install it again | `proxy/pluginstore.py` |
| `invalid` | 422 | its id is not a provider id | `proxy/configv2.py`, `proxy/hookapi.py` and 5 more |
| `invalid_document` | 422 | {...}: {...} | `proxy/settingscopes.py` |
| `key_exists` | 409 | the key sha256:{...} is trusted already (id {...}): remove it first to change its limits | `proxy/plugintrust.py` |
| `kind_unavailable` | 422 | kind {...} is not available in this version: it arrives with {...}. Use http, builtin or plugin. | `proxy/hookstore.py` |
| `lan_denied` | 409 | macOS keeps this app off the local network: turn it on in {...}, then find again | `proxy/discovery.py` |
| `last_owner` | 409 | this is the last enabled owner: make another owner first | `proxy/accounts.py`, `proxy/authapi.py` |
| `length_required` | 411 | send a Content-Length | `tandemapp/chat_forward.py` |
| `limit_over_role` | 400 | {...}: at most {...} for a token of a {...} | `proxy/tokens.py` |
| `link_timeout` | 504 | the stages are up but not linked after {...} s: {...} | `proxy/splitrun.py` |
| `locked` | 422 | {...} cannot be changed here: {...} | `proxy/settings.py`, `proxy/settingscopes.py` and 1 more |
| `no_agent` | 409 | {...} is read without its agent | `proxy/fleetupdate.py`, `proxy/fleetweights.py` |
| `no_engine` | 409, 503 | no TandemLLM engine on this node serves the Setup Guide | `proxy/deployments.py`, `proxy/nodeagent.py` |
| `no_http` | 409 | a hot change goes through the engine's HTTP route, which this controller cannot reach | `proxy/settingsengine.py` |
| `no_launcher` | 409 | TandemApp's chatd launcher {...} is not here (a package without ops/chatd.sh) | `proxy/nodechat.py` |
| `no_package` | 409 | no package {...}: send it first (PUT /node/update/package) | `proxy/nodeupdate.py` |
| `no_plan` | 409 | this Split has no plan | `proxy/splitrun.py` |
| `no_restart` | 404 | no restart {...} | `proxy/settingsengine.py` |
| `no_scope` | 404 | {...}: no scope {...} (one of {...}) | `proxy/settings.py` |
| `no_secret` | 409 | only an http hook has a secret | `proxy/hookstore.py` |
| `no_space` | 507 | needs {...} GiB and 1 GiB to spare, {...} GiB free | `proxy/nodeweights.py` |
| `no_target` | 404 | no plugin is installed: install one on /v1/fleet/plugins, then its settings are here | `proxy/pluginsettings.py`, `proxy/settings.py` |
| `no_version` | 404 | no version {...} of the {...} scope | `proxy/settings.py`, `proxy/settingscopes.py` and 1 more |
| `not_a_node` | 409 | {...} is a backend without TandemNode: a Split's stages run on nodes | `proxy/deployments.py` |
| `not_answering` | 500 | {...} answers but has no certificate at {...} yet | `proxy/nodeagent.py` |
| `not_built` | 409, 501 | setting link addresses with sudo comes with the rented pair (step 7): set the fabric up by hand, then test it here | `proxy/addspark.py` |
| `not_empty` | 409 | this controller has a fleet of its own: an import lands only on an empty one | `proxy/ctlmove.py`, `proxy/nodeagent.py` |
| `not_managed` | 409 | {...} is not managed by TandemNode and does not answer | `proxy/deployments.py`, `proxy/nodeengines.py` and 3 more |
| `not_ours` | 409 | {...} has no stop command and was not started by this agent | `proxy/nodeengines.py` |
| `not_owned` | 409 | chatd is not this controller's: start it with `tandem-app serve --chat own` (TANDEM\_CHAT=own) | `proxy/appchat.py`, `proxy/nodechat.py` |
| `not_requested` | 422 | {...} asks trust {...}, not {...} | `proxy/pluginstore.py` |
| `own_account` | 409 | change your own password with the current one | `proxy/authapi.py` |
| `owned` | 409 | owned by {...} (sha256 {...}); `tandem node takeover` moves it to another TandemApp, or release it there | `proxy/nodeagent.py` |
| `partial_split` | 409 | {...} · and {...} could not be stopped: Stop ends it | `proxy/splitrun.py` |
| `password_change_required` | 403 | set a new password first (PUT /v1/auth/password) | `proxy/accounts.py` |
| `password_first` | 409 | set the owner's password first: local mode trusts this machine only | `proxy/authapi.py`, `proxy/settingscopes.py` and 1 more |
| `password_needed` | 409 | the new controller listens on the LAN: give it an admin password ({...} or more characters, no spaces) | `proxy/ctlmove.py` |
| `permission_denied` | 403 | this needs the permission {...} | `proxy/accounts.py`, `proxy/app.py` and 6 more |
| `permission_error` | in the body |   | `proxy/relay.py` |
| `pin_failed` | 502 | {...} did not pin {...}: HTTP {...} | `proxy/fleetweights.py` |
| `plugin_down` | 503 | no plugin host here | `proxy/chatengine.py`, `proxy/pluginhost.py` and 1 more |
| `plugin_error` | 502 | backend/request answers {status, body} or {status, events} | `proxy/pluginhost.py` |
| `plugin_refused` | 422 |   | `proxy/pluginstore.py` |
| `preflight_failed` | 409 | {...}: fix the red checks, then check again | `proxy/addspark.py` |
| `question_taken` | 409 | the provider {...} serves {...} {...}: one provider a question on or ab (any number in shadow) | `proxy/providerstore.py` |
| `read_only` | 409 | this server's conductor documents are read-only | `proxy/settings.py`, `proxy/settingscopes.py` and 1 more |
| `referenced` | 409 | {...} is named by {...} hook(s): {...}. Remove those hooks first | `proxy/pluginstore.py` |
| `refused` | 403, 502 | the source gave HTTP {...} for {...} | `proxy/fleetweights.py`, `proxy/pluginhost.py` |
| `rejoin_timeout` | 504 | {...} is not {...} {...} s after the update | `proxy/fleetupdate.py` |
| `replica_down` | 409 | no other member of {...} serves: draining {...} would stop it | `proxy/fleetupdate.py` |
| `request_timeout` | 408 | the request body stopped arriving | `tandemapp/chat_forward.py` |
| `restore_invalid` | 422 | version {...} is not valid now: {...} | `proxy/settingscopes.py`, `proxy/settingsengine.py` |
| `secret_needed` | 409 | enter the {...} for {...}@{...} again: it is kept in memory only, for {...} minutes, and never stored | `proxy/addspark.py` |
| `send_failed` | 502 | {...}'s agent took no package: HTTP {...} {...} | `proxy/fleetupdate.py` |
| `service_identity` | 409 | a service identity is managed by TandemApp itself | `proxy/tokenapi.py` |
| `setup_code_spent` | 410 | no setup code is valid now: print a new one with `tandem-app users setup-code` on the machine, or restart | `proxy/authapi.py` |
| `short_body` | 400 | {...}: {...} of {...} bytes arrived | `proxy/nodeweights.py` |
| `short_read` | 502 | {...}: the source gave {...} of {...} bytes | `proxy/nodeweights.py` |
| `sign_in_required` | 401 | sign in, or send an API token | `proxy/accounts.py` |
| `signature_required` | 409 | trust engine takes a signature by a key trusted for engine, and {...} | `proxy/pluginstore.py` |
| `ssh_failed` | 502 | ssh to {...} failed: {...} | `proxy/addspark.py` |
| `stage_failed` | 502 | {...}: its agent did not take the stage: {...} | `proxy/splitrun.py` |
| `stage_mismatch` | 502 | {...} started {...}, not the {...} stage {...} | `proxy/splitrun.py` |
| `stale_plan` | 409 | the state changed since this plan, or the confirm is not its: plan again | `proxy/appparts.py`, `proxy/settings.py` and 1 more |
| `start_failed` | 502 | {...}: {...} | `proxy/deployments.py`, `proxy/splitrun.py` |
| `start_timeout` | 504 | {...} is not up after {...} s | `proxy/deployments.py`, `proxy/splitrun.py` |
| `stop_failed` | 502 | {...}: {...} | `proxy/deployments.py`, `proxy/splitrun.py` |
| `stop_timeout` | 504 | {...} still answers | `proxy/deployments.py`, `proxy/splitrun.py` |
| `stopped` | 409 | the copy was stopped | `proxy/nodeweights.py` |
| `target_required` | 400 | the {...} scope has {...} targets: name one ({...}) | `proxy/settings.py` |
| `token_refused` | 403 | the {...} settings take a signed-in account, never an API token | `proxy/app.py`, `proxy/settingscopes.py` |
| `too_late` | 409 | the engine is {...}: a restart that acts cannot be cancelled | `proxy/settingsengine.py` |
| `trust_confirm_needed` | 409 |   | `proxy/pluginstore.py` |
| `trust_required` | 403 | {...} take pairing's rights: set an admin password (App settings) to do this from another machine, or ask from this one | `proxy/hookapi.py`, `proxy/pluginsettings.py` and 1 more |
| `typed_confirm_mismatch` | 422 | confirm\_trust is not the plugin's id: type {...} exactly | `proxy/pluginstore.py`, `proxy/plugintrust.py` |
| `typed_confirm_required` | 409, 428 | trust {...} for {...} takes the typed confirmation: type the plugin's id ({...}) as confirm | `proxy/pluginstore.py`, `proxy/plugintrust.py` |
| `unavailable` | 503 | the config database is not open | `proxy/hookstore.py`, `proxy/pluginapi.py` and 3 more |
| `unknown_key` | 422 | not a key of the {...} scope: {...} | `proxy/settings.py`, `proxy/settingscopes.py` and 1 more |
| `unmapped_route` | 403 | no permission is defined for this route | `proxy/accounts.py` |
| `unsupported_media_type` | 415 | the body is the wheel's bytes (Content-Type: application/octet-stream) | `proxy/pluginapi.py` |
| `unsupported_parameter` | in the body |   | `proxy/translate/anthropic.py` |
| `update_timeout` | 504 | {...} does not run {...} after {...} s | `proxy/fleetupdate.py` |
| `upload_too_large` | 413 | a wheel is at most {...} MiB | `proxy/pluginapi.py` |
| `version_mismatch` | 409, 500 | {...}: {...}; Install refuses another version | `proxy/addspark.py`, `proxy/deployments.py` and 1 more |
| `version_unchanged` | 409 | {...} {...} is installed with another tree (sha256 {...}, this one {...}): a changed plugin carries a new version | `proxy/pluginstore.py` |
| `view_hidden` | 403 | the page {...} is hidden from the role {...} | `proxy/accounts.py`, `proxy/settings.py` |
| `warm_timeout` | 504 | no token after {...} s: {...} | `proxy/deployments.py` |
| `wrong_credentials` | in the body | the username or the password is not right | `proxy/authapi.py` |
| `wrong_setup_code` | 401 | the setup code is not right | `proxy/authapi.py` |

<!-- /generated -->

