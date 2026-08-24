# Client credential projections

Projection modules are shared adapters over the
`ClientCredentialProvider` capability. Extensions declare which projections
they support; nodes bind a projection instead of interpreting credential
material directly.

## Compatibility

| Extension provider | Authorization header | Bearer access token | Why |
| --- | --- | --- | --- |
| `azure_identity_auth` | Supported | Supported | Produces OAuth bearer tokens, which can be formatted as headers or passed to a token-based SDK. |
| `oauth2_client_auth` | Supported | Supported | Produces OAuth bearer tokens, which can be formatted as headers or passed to a token-based SDK. |
| Future `basic_auth` | Supported | Rejected | Produces a username/password pair. It can form a Basic header but cannot satisfy an SDK contract requiring an OAuth bearer access token. |

`azure_identity_auth` and `oauth2_client_auth` are wired in the prototype.
`basic_auth` shows the intended compatibility for a future extension.

An unsupported projection is rejected by the projection module's `bind`
function during node construction:

```text
BasicAuth -> bearer_access_token::bind() -> UnsupportedProjection
```

This is not a missing conversion. Converting a username/password pair into a
bearer token would require an authentication exchange with a token service,
which is a different extension such as `oauth2_client_auth`. Treating Basic
material as a bearer token would mislabel the credential and defer the error to
the remote service.

## Ownership

```text
Extension
  -> acquires and refreshes typed credentials
  -> implements ClientCredentialProvider

Projection module
  -> validates compatibility
  -> formats or adapts credentials
  -> owns per-consumer state

Node
  -> resolves ClientCredentialProvider
  -> binds one approved projection
```

New credential material is added to `CredentialMaterial`. Each projection must
explicitly opt in before an extension can advertise that use. New projections
belong in this directory and should reject all credential material by default
until support is implemented and tested.
