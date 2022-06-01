# Building the cluster

Here's an excerpt of how to add 2 OSDs using Ceph's Orchestrator API:
```bash
# >> Convenience variables
osd_host="vagrant"
osd_dev1="/dev/sdb"
osd_dev2="/dev/sdb"

# >> Add 2 OSDs on the same host (just to test things out)
ceph orch daemon add osd "${osd_host}:${osd_dev1}"
ceph orch daemon add osd "${osd_host}:${osd_dev2}"
```
