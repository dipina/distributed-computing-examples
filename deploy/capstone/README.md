# Deploying the capstone: Docker and AWS (ECS Fargate)

This folder takes the **Smart Weather Alert Platform** of
[`examples/24_capstone_smart_weather_platform`](../../examples/24_capstone_smart_weather_platform)
from "processes on my laptop" to **containers** and then to the **cloud**. The code is the same at all three
stages; only the way it is packaged, supervised and reached changes.

| Stage | How it runs | Who restarts crashed components (example 22) | How you reach it |
|---|---|---|---|
| 1. Laptop | `demo.py` spawns ~10 OS processes | the Python `Supervisor` in `demo.py` | `127.0.0.1` |
| 2. Docker Compose | 4 containers + a shared volume | Docker: `restart: unless-stopped` + healthchecks | `127.0.0.1:8080` / `:50051` |
| 3. AWS ECS Fargate | 1 Fargate task with 4 containers + a task volume | the ECS **service** (desired count = 1) | the task's public IP |

```
deploy/capstone/
├── Dockerfile           one image for every component (the command selects which one runs)
├── requirements.txt     runtime dependencies of the image
├── docker-compose.yml   local multi-container deployment (+ "sensors" profile)
├── chaos.py             kills the component inside a container (to watch the restart)
├── smoke_test.py        end-to-end test from your laptop against ANY host (local or AWS)
└── aws/
    ├── ecs-fargate.yaml CloudFormation: ECS cluster, task definition, service, security group, logs
    └── deploy.py        up | status | chaos | down  (Windows, macOS and Linux; uses the AWS CLI + Docker)
```

---

## 1. Prerequisites

| Tool | Needed for | Install |
|---|---|---|
| Docker Desktop / Engine (with Compose v2) | stages 2 and 3 | <https://docs.docker.com/get-started/get-docker/> |
| AWS CLI v2 | stage 3 | <https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html> |
| Python ≥ 3.10 + `pip install grpcio grpcio-tools httpx` | `smoke_test.py` (acts as sensors, dashboard, analyst and AI agent) | – |

Check with `docker compose version` and `aws --version`.

---

## 2. Containers with Docker Compose

Run everything from the **repository root**:

```bash
# build the image and start ingest, api, analytics-0 and analytics-1 (all healthy in ~10 s)
docker compose -f deploy/capstone/docker-compose.yml up -d --build
docker compose -f deploy/capstone/docker-compose.yml ps

# generate traffic from containers (4 sensors streaming over gRPC)...
docker compose -f deploy/capstone/docker-compose.yml --profile sensors run --rm sensors

# ...or run the full end-to-end test from your machine (sensors + SSE + GraphQL + MCP agent)
python deploy/capstone/smoke_test.py --host 127.0.0.1

# explore
#   http://127.0.0.1:8080/graphql          GraphiQL: try { stations { city mean max alerts { level celsius } } }
#   curl -N http://127.0.0.1:8080/alerts/stream
docker compose -f deploy/capstone/docker-compose.yml logs -f analytics-0

# chaos: kill a worker inside its container, then watch Docker restart it
docker compose -f deploy/capstone/docker-compose.yml exec analytics-0 python /app/chaos.py
docker compose -f deploy/capstone/docker-compose.yml logs analytics-0 | grep resuming
#   -> "resuming from committed offsets {0: 23, 2: 44}"  (nothing lost, nothing counted twice)

# clean up (-v also deletes the data volume)
docker compose -f deploy/capstone/docker-compose.yml down -v
```

Points to notice:

* **One image, many roles.** The same image runs every component; the `command` picks the script.
  This is common for small platforms, and it keeps the gRPC stubs, which are compiled at build time, identical everywhere.
* **Supervision moves into the runtime.** `restart: unless-stopped` and `healthcheck` do what `Supervisor` did in
  `demo.py`. `init: true` puts `tini` in front of Python as PID 1, so signals and crashes behave as expected.
* **Shared state = shared volume.** The commit log files and the SQLite read model live in the `data` volume.
  That is why all components must run on the **same host**; see §4 for how to remove this limit.
* To avoid Docker Hub rate limits, build from the ECR Public mirror:
  `BASE_IMAGE=public.ecr.aws/docker/library/python:3.12-slim docker compose -f deploy/capstone/docker-compose.yml build`

---

## 3. AWS: ECS on Fargate

**Fargate** runs containers without servers to manage (serverless containers, slide 72). The CloudFormation template
creates:

* an **ECS cluster**, and a **task definition** with the 4 components plus a one-shot `init-data` container.
  Fargate bind mounts are root-owned, so `init-data` hands `/data` to the non-root app user. Container
  **dependencies** start the workers only when ingest is HEALTHY.
* an **ECS service** (DesiredCount = 1). This is the cloud reconciliation loop: if an essential container dies, ECS replaces the task.
* a **security group** that opens 8080 (GraphQL/SSE) and 50051 (gRPC) **only to your public IP** (/32).
* a **CloudWatch Logs** group (`/ecs/<stack>`, 7-day retention), and optionally a task execution role.

### 3.1 Your own AWS account

```bash
aws configure                                  # access key, secret, region (e.g. eu-west-1)
python deploy/capstone/aws/deploy.py up        # ~5 min: ECR repo, build + push, stack, wait until stable
```

`up` prints the endpoints:

```
  GraphQL (GraphiQL in a browser) : http://3.91.10.20:8080/graphql
  SSE alert stream                : curl -N http://3.91.10.20:8080/alerts/stream
  gRPC ingest                     : 3.91.10.20:50051
  End-to-end smoke test           : python deploy/capstone/smoke_test.py --host 3.91.10.20
  Logs                            : aws logs tail /ecs/weather-platform --follow --region eu-west-1
```

Then run:

```bash
python deploy/capstone/smoke_test.py --host <public-ip>    # your laptop = edge devices + clients
python deploy/capstone/aws/deploy.py status                # IP and stack status again
python deploy/capstone/aws/deploy.py chaos                 # stop the task; ECS starts a new one
python deploy/capstone/aws/deploy.py down --purge-images   # delete EVERYTHING (stack + ECR repo)
```

Useful options: `--region`, `--stack NAME`, `--arch arm64` (Graviton, cheaper; the image is built for `linux/arm64`),
`--allowed-cidr 0.0.0.0/0` (e.g. for a whole classroom), and `--base-image public.ecr.aws/docker/library/python:3.12-slim`.

### 3.2 AWS Academy Learner Lab

Learner Lab accounts cannot create IAM roles, but they include **`LabRole`**. `deploy.py` detects it and passes it
as the task execution role (`--lab-role` forces this). Steps:

1. Start the lab and open **AWS Details → AWS CLI**. Copy the credentials into `~/.aws/credentials`
   (Windows: `%USERPROFILE%\.aws\credentials`). The region is **us-east-1**.
2. `python deploy/capstone/aws/deploy.py up --region us-east-1`
3. Credentials expire when the lab session ends (about 4 h). Run `down` before you leave to save budget.

### 3.3 Cost and safety

* One task with 1 vCPU and 2 GB costs roughly **5 US cents per hour** on x86 (less on ARM64), plus a small charge for the public IPv4 address,
  CloudWatch Logs and ECR storage. Check <https://aws.amazon.com/fargate/pricing/>. **Always run `down` afterwards.**
* This is a **teaching deployment**. The endpoints have **no TLS and no authentication**, and they are only restricted by IP.
  For production, put an **Application Load Balancer** with HTTPS (ACM certificate) in front of the API, use a **Network
  Load Balancer** or an ALB with HTTP/2 for gRPC, and add authentication (Cognito, or the API key and rate limiting from example 15).
* The task volume is **ephemeral**. When ECS replaces the task (for example after `chaos` or a crash), the log and read model start empty
  and the **public IP changes**. The Docker Compose volume, by contrast, survives restarts.

---

## 4. From "one task" to a cloud-native architecture (discussion and exercises)

The deployment keeps the capstone's code unchanged, so all components share one filesystem and therefore **one
Fargate task**. To scale each component independently you replace the local shared state with **managed services**;
the component code stays almost the same:

| Capstone piece | Now | Managed AWS service | What changes in the code |
|---|---|---|---|
| Commit log (17) | files in `/data` | **Amazon MSK** (Kafka) or **Kinesis Data Streams** | `CommitLog` → Kafka producer/consumer (`confluent-kafka`) |
| Read model + offsets | SQLite | **DynamoDB** (transactions) or **RDS/Aurora PostgreSQL** | `ReadModel` → boto3 / psycopg; keep "view + offset in one transaction" |
| API (GraphQL + SSE) | 1 container | ECS service behind an **ALB** (or **AWS AppSync** for managed GraphQL) | none |
| Ingest (gRPC) | 1 container | ECS service behind an **NLB** (or ALB with gRPC) | none |
| Supervisor (22) | ECS service | one ECS service per component, with **Service Auto Scaling** | none |
| Durable state for the demo | ephemeral volume | **Amazon EFS** volume in the task definition | none (single writer host only: SQLite over NFS needs care) |

Exercises:

1. Add an **EFS** volume to `ecs-fargate.yaml` (file system, mount targets, security group for NFS 2049) so the data
   survives `deploy.py chaos`.
2. Split the task into **two services** (ingest + workers, api) sharing EFS. Which consistency problems appear with SQLite?
3. Replace `CommitLog` with Kafka (`docker run apache/kafka` locally, then Amazon MSK Serverless), and scale the consumer
   group to 3 workers.
4. Put the API behind an ALB with a health check on `/health`, then change the security group so only the ALB can reach port 8080.
5. Add a **GitHub Actions** workflow that builds the image, pushes it to ECR and runs `aws cloudformation deploy` on every tag.

---

## 5. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `exec format error` in the task logs | Image built for the wrong CPU (e.g. on Apple Silicon). `deploy.py` builds `--platform linux/amd64` by default; pass `--arch arm64` to use Graviton instead. |
| `toomanyrequests` while building | Docker Hub rate limit: use `--base-image public.ecr.aws/docker/library/python:3.12-slim`. |
| `smoke_test.py` cannot connect | Your IP changed (VPN, eduroam...) and the security group only allows the old one: re-run `deploy.py up --allowed-cidr <new-ip>/32`. |
| Task stops right after starting | `aws logs tail /ecs/weather-platform --region <r>`; look at the `init`, `ingest` and `api` streams. |
| `AccessDenied` creating a role | Learner Lab or a restricted account: use `--lab-role`, or ask for a task execution role and pass its ARN via the template parameter. |
| Windows: `aws` not found | Reopen the terminal after installing the AWS CLI MSI, or add `C:\Program Files\Amazon\AWSCLIV2` to `PATH`. |

References: [ECS task definitions](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definitions.html) ·
[Fargate task storage](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-task-storage.html) ·
[Bind mounts](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/bind-mounts.html) ·
[Pushing to ECR](https://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html) ·
[Compose file reference](https://docs.docker.com/reference/compose-file/) ·
[Dockerfile best practices](https://docs.docker.com/build/building/best-practices/)
