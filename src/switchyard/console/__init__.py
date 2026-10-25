"""The Switchyard console: a live view of what the gateway is doing.

Runs as a separate ASGI app in the same image and package, holding the admin
credential server-side so the browser never sees one. It reads the same
numbers Grafana reads; the difference is the audience. Grafana is the
operator's metrics tool. The console tells the story the metrics are evidence
for -- which tenant is being protected from which, and what happened when a
provider died.
"""
