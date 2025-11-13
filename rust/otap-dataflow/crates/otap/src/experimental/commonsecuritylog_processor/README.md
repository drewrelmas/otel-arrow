# CommonSecurityLog Processor

**Status:** IN DEVELOPMENT

This processor is designed as a post-processor on the existing
[syslog_cef_receiver](../../syslog_cef_receiver.rs) to convert that receiver's
output into a data format representative of Microsoft's [CommonSecurityLog
schema
definition](https://learn.microsoft.com/en-us/azure/azure-monitor/reference/tables/CommonSecurityLog).

Since we are operating over OpenTelemetry-style data - the 'flat' table schema will be projected into the `LogRecord` `attributes` map. For example, the CommonSecurityLog column `DeviceAction` will map to `Attributes['DeviceAction']`.
