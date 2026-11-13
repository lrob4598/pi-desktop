# Pi-Native telemetry source

These source files are copied from the Human Pi-Native repository at commit
`c1a3faf5084d1d0673d2c2ec25b820082faf6fe4`. They supply the telemetry host,
protocol, and client used by the native Crystal integration and its tests.

`packages/pi-host/src/telemetry-store.ts`, the Crystal telemetry contract helper,
and the vendored contract/hub tests were updated from Pi-Native merge commit
`a67887103c3140f89a88bad4521e62780634f03b` to include workspace-reassignment
project isolation and old-subscriber closure. Other files retain the original pin.
