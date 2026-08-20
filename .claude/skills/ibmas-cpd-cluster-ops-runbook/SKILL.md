---
name: ibmas-cpd-cluster-ops-runbook
description: "Use for IBM Cloud Pak for Data / Software Hub platform operations on OpenShift — checking cluster/CPD version, pod health, operator status, namespace inventory, safe restart/shutdown procedures, and how to actually reach the cluster (oc vs bastion). Triggers on phrases like: 'is CPD healthy', 'check cluster version', 'restart CPD', 'stuck pods', 'delete pod', 'operator not ready', 'bastion', 'oc login', 'CPD 5.3'."
---

# CPD / OpenShift Cluster Ops Runbook

## Access model (confirm before assuming a bastion is needed)

Day-to-day ops use **direct `oc`/`kubectl`/`cpdctl` from the workstation** —
already authenticated (`oc whoami` → `kube:admin`) against
`https://api.<cluster-app-domain>:6443`. This is the primary and normal path.

A bastion exists (`~/.ssh/config` host `ibmas-bastion`, IP `9.82.206.23`, key
`~/.ssh/ibmas_bastion_rsa`) labeled "OpenShift Installer" — treat it as **secondary,
rare**: node-level SSH access `oc` can't provide (e.g. debugging kubelet/cri-o on a node
directly), or emergency access if the API server itself is unreachable. Note a
documented discrepancy: `.env.example` describes the bastion SSH user as `cpadmin`
(the CPD app admin), while the local `~/.ssh/config` entry uses `ibmas` with a
dedicated key — confirm which is live before using the bastion; don't assume.

Never invent SSH steps beyond what's documented here.

## Checking versions (read-only, always safe)

```bash
oc get clusterversion                                   # OpenShift version
oc get wxd -A                                            # watsonx.data version + size
oc get wxdengine -A                                       # Presto/Milvus engine versions
oc get wkc.wkc.cpd.ibm.com -A                              # WKC/IKC version
oc get datastages.ds.cpd.ibm.com -A                        # DataStage version
oc get pxruntimes.ds.cpd.ibm.com -A                        # DataStage runtime version
oc get analyticsengines -A                                 # Spark (Analytics Engine) version
oc get clusters.opensearch.cloudpackopen.ibm.com -A        # OpenSearch version
```
This cluster (verified, re-check before customer use): OpenShift 4.19.40, CPD stack
5.3.x (WKC 5.3.17, DataStage/pxruntime 5.3.6, AnalyticsEngine 5.3.6), watsonx.data 2.3.4
non-Premium.

## Key namespaces

| Namespace | Contents |
|---|---|
| `cpd-instance` | All CPD operands: wxd*, wkc-cr, datastage, pxruntime, analyticsengine, elasticsearch-master, EDB/Crunchy PG clusters |
| `cpd-operators` | Controller managers for the above |
| `ibm-cpd-scheduler` | Scheduling operator |
| `ibm-licensing` | IBM Licensing operator |

## The wxd addon-shutdown hazard (learned from a live incident)

Presto's `wxdengine` has `shutdown_by_addon=true` — **the `wxdAddon` Ansible operator is
the controlling authority**, not the `wxd` CR directly. A bare
`oc patch wxd lakehouse -p '{"spec":{"shutdown":true}}'` then `false` does **not**
reliably restart wxd: it can scale the metastore/MinIO/CAS/CES layer to `0/0` and
`shutdown=false` won't bring it back if the addon's startup playbook is wedged
(`status.conditions: Failure "unknown playbook failure"`).

**Recovery that worked:** full addon cycle (shutdown=true → wait for quiesce →
shutdown=false) **plus** restart the wedged operator so its playbook re-runs clean:
```bash
oc delete pod -n cpd-operators -l control-plane=ibm-lakehouse-controller-manager
```
This is why the project's `scripts/cpd_maintenance.sh restart` action uses
`oc rollout restart` on `ibm-lh-lakehouse-*` workloads and **never toggles
`spec.shutdown` directly**.

## The "0/0 reads healthy" readiness trap

After a real shutdown, every Deployment/StatefulSet scan reports green because
`ready == desired == 0`. The only authoritative signal is the `spec.shutdown` **string**
flag on both `wxd` and `wxdaddon` (see `ibmas-watsonxdata-engine-control`) — never infer
health from replica counts alone.

## `scripts/cpd_maintenance.sh` — the existing runbook automation

```
status | verify | shutdown | startup | restart | prepare-upgrade | resume-upgrade | drain-node | uncordon-node
```
- `verify` runs `scripts/lib/readiness.sh`: parses `oc get -o json`, excludes Job pods,
  distinguishes Terminating-surge from STUCK (checks `deletionTimestamp`), checks
  Deployments (incl. `observedGeneration`), StatefulSets (OnDelete-aware), EDB CNPG
  (`readyInstances==spec.instances` AND `phase=="Cluster in healthy state"`), and
  FoundationDB (`status.health.available/healthy/fullReplication`). Fails **closed** if
  `oc` returns no pods.
- `--dry-run` routes every mutating action through a `run()` wrapper that only prints —
  always preview `prepare-upgrade`/`resume-upgrade` first.
- `prepare-upgrade` is targeted: pauses only `ibm-cpd-wkc-operator` +
  `ibm-cpd-datastage-operator`, scales only wkc/datastage/ds-px/Spark/wxd CRDs to 0,
  leaves Zen/console/IAM/scheduler/CCS/EDB/FDB up so node drains evict fewer slow-probe
  pods. State is saved to `logs/*.state` for exact restore.

## `ccs-cr` immutable-StatefulSet-field failure (recurring, usually self-heals)

`ccs-cr` (`cpd-instance`) occasionally fails a reconcile with `"Create statefulset for
rabbitmq"` → HTTP `422 FieldValueForbidden` on StatefulSet `rabbitmq-ha` ("updates to
statefulset spec for fields other than replicas, ordinals, template, updateStrategy,
persistentVolumeClaimRetentionPolicy and minReadySeconds are forbidden"). This cascades:
`wkc-cr` reports "Dependency CCS failed to install" and blocks all IKC/WKC work.

**Don't diagnose from the pod list or `.status.diagnosticStatus`** — both can look green
while this is active (pods stay untouched by the rejected patch; diagnosticStatus has
misreported unrelated Completed cronjob pods as "not Ready" in the past). Read
`.status.reconcileHistory` and `.status.conditions[?(@.type=="Failure")].message` instead:
```bash
oc get ccs ccs-cr -n cpd-instance -o jsonpath='{.status.reconcileHistory}'
```

**This failure class recurs** (seen 2026-07-14→08-06 stuck at 31.2%, then again as a single
transient entry on 2026-08-19) — treat it as expected periodic operator behavior, not a
one-off. **It usually self-heals within a few reconcile cycles** (~8 min, confirmed
2026-08-19) without intervention. Only if it stays wedged at a fixed percentage for more
than a few cycles, fall back to the manual fix (zero downtime, pods are adopted, not
restarted):
```bash
oc -n cpd-instance delete statefulset rabbitmq-ha --cascade=orphan
```
If `wkc-cr` doesn't clear on its own cycle once CCS recovers, nudge it:
```bash
oc patch wkc wkc-cr -n cpd-instance --type merge --patch '{"spec":{"dummyone":true}}'
```

## Pod-deletion triage (the "just delete the pod" ask)

1. `oc get pods -n cpd-instance | grep -vE 'Running|Completed'` — find the actual problem
   pod, don't guess.
2. `oc describe pod <name> -n cpd-instance` — read Events, not just status.
3. **Low-risk, auto-run**: deleting a single pod that is already `CrashLoopBackOff`/`Error`
   *within* a Deployment/StatefulSet's own desired replica count — the controller
   recreates it, this is a normal recovery move.
4. **High-risk, confirm first**: deleting an operator/controller pod (like the
   `ibm-lakehouse-controller-manager` fix above), deleting anything stateful (PG/EDB/FDB,
   MinIO), or bulk-deleting more than one pod at a time.

## Safety tiering for this skill

- **Auto-run**: all `oc get`/`describe`/`logs`, `cpd_maintenance.sh status`/`verify`,
  single crash-looping pod deletion.
- **Confirm first, always**: `shutdown`/`startup`/`restart`/`prepare-upgrade`/`resume-upgrade`
  actions, `drain-node`, any CR `spec` patch, any multi-pod or stateful-service deletion.
