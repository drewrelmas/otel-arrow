# CommonSecurityLog Processor

**Status:** IN DEVELOPMENT

This processor is designed as a post-processor on the existing
[syslog_cef_receiver](../../syslog_cef_receiver.rs) to convert that receiver's
output into a data format representative of Microsoft's [CommonSecurityLog
schema
definition](https://learn.microsoft.com/en-us/azure/azure-monitor/reference/tables/CommonSecurityLog).

Since we are operating over OpenTelemetry-style data - the 'flat' table schema
will be projected into the `LogRecord` `attributes` map. For example, the
CommonSecurityLog column `DeviceAction` will map to
`Attributes['DeviceAction']`.

While the majority of the conversion from `syslog_cef_receiver` output to
`CommonSecurityLog` entails simple renames of [well-known Extension
keys](https://learn.microsoft.com/en-us/azure/sentinel/cef-name-mapping) there
are a few exceptions that require more complex computation. For example:

| Column | Computed Value |
|--------|----------------|
| `AdditionalExtensions` | String concatenation of all non-well-known Extension key/values into a single value. |
| `RemoteIP` / `SourceIP` / `RemotePort` / `SourcePort` | Computed based on `CommunicationDirection` value |
| `SimplifiedDeviceAction` | Computed based on `DeviceAction` value |

Therefore, this processor is implemented separately from
[attributes_processor](../../attributes_processor.rs), while still re-using
internal implementation as it makes sense.

It is possible that a more general purpose transform engine (like that under
development at
[experimental/query_engine](../../../../../../experimental/query_engine)) may
fulfill these requirements and this specific processor could be deprecated.
