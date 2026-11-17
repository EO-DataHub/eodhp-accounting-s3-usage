# UK EO Data Hub Platform: eodhp-accounting-s3-usage

This collects accounting events relating to S3 storage use by workspaces and S3 protocol-based
access to those stores. Access via HTTPS is collected via the data transfer billing collector
and CloudFront logs.

## Requester-pays reads

It also charges workspaces for reads of requester-pays buckets, such as `usgs-landsat`, through
the data-objects service (ADR-003 in eodhp-guide). The Hub pays AWS for these reads.

- The data-objects service signs each read as a session of its signer role, named after the
  workspace. The account's data-access CloudTrail trail records each read.
- For each period, one Athena query over the trail totals `bytesTransferredOut` per workspace.
  It counts only successful `GetObject` events signed by this cluster's signer role. Staging and
  prod share an AWS account, and the thumbnail role's reads are previews that the project pays for.
- Each workspace with reads gets one `BillingEvent` with SKU `REQUESTER-PAYS-DATA-TRANSFER`, in
  GB. The SKU needs a price in the accounting service's product configuration, or it is not charged.

| Variable | Example |
|---|---|
| `DATA_ACCESS_TRAIL_TABLE` | `data_access_trail_test`. Leave unset to turn this off. |
| `DATA_ACCESS_TRAIL_S3_PREFIX` | `s3://eodhp-123456789012-data-access-trail/AWSLogs/123456789012/CloudTrail/` |
| `DATA_ACCESS_TRAIL_REGIONS` | `us-west-2` (the default). Comma-separated regions of the requester-pays buckets. |
| `DATA_OBJECTS_SIGNER_ROLE_NAME` | `data-objects-signer-<cluster name>` |

The collector creates the Athena table at startup if it does not exist. It does not change an
existing table, so drop the table after a change to its definition.

## When charges appear

The collector charges usage in fixed periods, set by `--interval` (4 hours in the deployment).
It totals a period only after `LOG_DELAY_BUFFER` (3 hours) has passed since the period ended:

- **The periods** keep the number of Athena queries and ledger rows small. Each workspace gets
  one event per SKU per period, not one per request.
- **The buffer** is for S3 server access logs, which the storage-access SKUs are charged from.
  AWS delivers those logs on a best-effort basis, sometimes hours late. A log line that arrives
  after its period was charged is never charged: a second event for that period would have the
  same UUID, and the accounting service ignores it.

So usage appears in the ledger 3 to 7 hours after it happens. For example, a read at 12:01 and a
read at 15:59 are both in the 12:00 to 16:00 period, which the collector totals at about 19:00.

Requester-pays reads share these settings because they run in the same loop. CloudTrail
delivers its data events in about 5 to 15 minutes, so those reads could be charged sooner with
their own, shorter period and buffer. That needs a code change.

After each run the collector carries on from its start time less the buffer. The period that
was still inside the buffer at that time is then totalled by the next run, also after a restart.

# Development of this component

## Getting started

### Install via makefile

```commandline
make setup
```

This will install [uv](https://docs.astral.sh/uv/), sync the project dependencies,
and install `pre-commit`.

It's safe and fast to run `make setup` repeatedly as it will only update these things if
they have changed.

After `make setup` you can run `pre-commit` to run pre-commit checks on staged changes and
`pre-commit run --all-files` to run them on all files. This replicates the linter checks that
run from GitHub actions.

## Building and testing

This component uses `pytest` tests, the `ruff` linter/formatter, and `pyright` type checker.

A number of `make` targets are defined:

- `make test`: run tests continuously
- `make testonce`: run tests once
- `make format`: format and fix lint issues
- `make check`: run linter and type checker
- `make dockerbuild`: build a `latest` Docker image (use `make dockerbuild VERSION=1.2.3` for a release image)
- `make dockerpush`: push a `latest` Docker image (again, you can add `VERSION=1.2.3`) - normally this should be done
  only via the build system and its GitHub actions.
- `make krestart`: runs kubectl to rolling-restart the service running in the cluster you're connected to

## Local Deployment

Before running this, ensure that you have setup the necessary prerequisites:

- A bucket to collect the logs in, e.g. `workspaces-access-logs-eodhp-ENVIRONMENT`. ArgoCD should have created this.
- The actual workspaces bucket has got "Server access logging" enabled and is distrubuting logs to `workspaces-access-logs-eodhp-ENVIRONMENT` with prefix `s3/standard/`. ArgoCD should have created this, but the dev environment's ArgoCD can't manage its bucket because it was created before ArgoCD was managing it. `standard` refers to standard S3 storage - if we later support, say, reduced redundancy storage then it will be billed at a different prices.

Then you can proceed to test the component.

```
k port-forward -n pulsar svc/pulsar-proxy 6650:6650 # in one terminal

python -m accounting_s3_usage.sampler --pulsar-url pulsar://localhost:6650 -v --once
```

## Managing requirements

Requirements are specified in `pyproject.toml`, with development requirements listed in
`[dependency-groups]`. After changing them, run `uv sync` to update the lockfile and
virtual environment.

To check for vulnerable dependencies, run `uv audit`.

## Releasing

Ensure that `make check` and `make testonce` work correctly and produce no further changes to code formatting before
continuing.

Releases tagged `latest` and targeted at development environments can be created from the `main` branch. Releases for
installation in non-development environments should be created from a Git tag named using semantic versioning. For
example, using

- `git tag 1.2.3`
- `git push --tags`

Normally, Docker images will be built automatically after pushing to the UKEODHP repos. Images can also be created
manually in the following way:

- For versioned images create a git tag.
- Log in to the Docker repository service. For the UKEODHP environment this can be achieved with the following command
  `AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=... aws ecr get-login-password --region eu-north-1 | docker login --username AWS --password-stdin 312280911266.dkr.ecr.eu-west-2.amazonaws.com`
  You will need to create an access key for a user with permission to modify ECR first.
- Run `make dockerbuild` (for images tagged `latest`) or `make dockerbuild VERSION=1.2.3` for a release tagged `1.2.3`.
  The image will be available locally within Docker after this step.
- Run `make dockerpush` or `make dockerpush VERSION=1.2.3`. This will send the image to the ECR repository.
