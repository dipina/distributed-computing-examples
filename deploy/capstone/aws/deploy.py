#!/usr/bin/env python3
"""Deploy the Smart Weather Alert Platform (example 24) to AWS ECS Fargate - cross-platform (Windows/macOS/Linux).

Prerequisites (on your machine):
    * AWS CLI v2, configured:  https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
          aws configure        (or, in AWS Academy Learner Lab: paste the credentials from "AWS Details")
    * Docker Desktop / Docker Engine running: https://docs.docker.com/get-started/get-docker/

Usage (from the repository root):
    python deploy/capstone/aws/deploy.py up        # build -> push to ECR -> CloudFormation -> print endpoints
    python deploy/capstone/aws/deploy.py status    # stack status + public IP + URLs
    python deploy/capstone/aws/deploy.py chaos     # stop the running task; watch ECS replace it
    python deploy/capstone/aws/deploy.py down      # delete the stack (add --purge-images to delete the ECR repo)

Options: --stack NAME (default weather-platform) --region REGION --arch amd64|arm64 --allowed-cidr CIDR
         --lab-role (use the AWS Academy 'LabRole'; auto-detected when present) --base-image IMAGE

References:
    - ECR: push a Docker image .. https://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html
    - aws cloudformation deploy .. https://docs.aws.amazon.com/cli/latest/reference/cloudformation/deploy/
    - ECS on Fargate ............. https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html
    - Fargate pricing ............ https://aws.amazon.com/fargate/pricing/
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = Path(__file__).with_name("ecs-fargate.yaml")
DOCKERFILE = ROOT / "deploy" / "capstone" / "Dockerfile"


def run(cmd: list[str], *, capture: bool = True, input_: str | None = None, check: bool = True) -> str:
    """Run a command, echo it, return stdout."""
    print("  $", " ".join(cmd), flush=True)
    p = subprocess.run(cmd, capture_output=capture, text=True, input=input_)
    if check and p.returncode != 0:
        sys.exit(f"command failed ({p.returncode}): {' '.join(cmd)}\n{(p.stderr or '').strip()}")
    return (p.stdout or "").strip()


def aws(*args: str, region: str, check: bool = True) -> Any:
    """Call the AWS CLI and parse its JSON output."""
    out = run(["aws", *args, "--region", region, "--output", "json"], check=check)
    return json.loads(out) if out else {}


def need(tool: str, url: str) -> None:
    """Fail early with an install hint if a CLI is missing."""
    if shutil.which(tool) is None:
        sys.exit(f"'{tool}' not found in PATH. Install it: {url}")


def default_region() -> str:
    """Region configured in the AWS CLI, or us-east-1 (AWS Academy Learner Lab default)."""
    p = subprocess.run(["aws", "configure", "get", "region"], capture_output=True, text=True)
    return p.stdout.strip() or "us-east-1"


def my_cidr() -> str:
    """This machine's public IP as a /32 (to restrict the security group)."""
    try:
        with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=5) as r:
            return r.read().decode().strip() + "/32"
    except OSError:
        print("  ! could not detect your public IP -> opening to 0.0.0.0/0 (use --allowed-cidr to restrict)")
        return "0.0.0.0/0"


def lab_role_arn(region: str) -> str | None:
    """ARN of the AWS Academy 'LabRole' if it exists in this account."""
    p = subprocess.run(["aws", "iam", "get-role", "--role-name", "LabRole", "--region", region, "--output", "json"],
                       capture_output=True, text=True)
    return json.loads(p.stdout)["Role"]["Arn"] if p.returncode == 0 else None


def build_and_push(region: str, repo: str, arch: str, base_image: str | None) -> str:
    """Create the ECR repo if needed, build the image for the Fargate CPU architecture, push it."""
    account = aws("sts", "get-caller-identity", region=region)["Account"]
    registry = f"{account}.dkr.ecr.{region}.amazonaws.com"
    if not aws("ecr", "describe-repositories", "--repository-names", repo, region=region, check=False):
        aws("ecr", "create-repository", "--repository-name", repo, "--image-scanning-configuration",
            "scanOnPush=true", region=region)
    password = run(["aws", "ecr", "get-login-password", "--region", region])
    run(["docker", "login", "--username", "AWS", "--password-stdin", registry], input_=password)
    tag = time.strftime("%Y%m%d-%H%M%S")
    uri = f"{registry}/{repo}:{tag}"
    build = ["docker", "build", "--platform", f"linux/{arch}", "-f", str(DOCKERFILE), "-t", uri]
    if base_image:
        build += ["--build-arg", f"BASE_IMAGE={base_image}"]
    run([*build, str(ROOT)], capture=False)
    run(["docker", "push", uri], capture=False)
    return uri


def default_network(region: str) -> tuple[str, list[str]]:
    """Default VPC and its public subnets."""
    vpcs = aws("ec2", "describe-vpcs", "--filters", "Name=isDefault,Values=true", region=region)["Vpcs"]
    if not vpcs:
        sys.exit("No default VPC in this region - create one (aws ec2 create-default-vpc) or edit the parameters.")
    vpc = vpcs[0]["VpcId"]
    subnets = aws("ec2", "describe-subnets", "--filters", f"Name=vpc-id,Values={vpc}",
                  "Name=map-public-ip-on-launch,Values=true", region=region)["Subnets"]
    return vpc, [s["SubnetId"] for s in subnets]


def outputs(stack: str, region: str) -> dict[str, str]:
    """CloudFormation stack outputs."""
    st = aws("cloudformation", "describe-stacks", "--stack-name", stack, region=region)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in st.get("Outputs", [])} | {"_status": st["StackStatus"]}


def public_ip(cluster: str, service: str, region: str) -> str | None:
    """Public IP of the running task (Fargate tasks get one ENI each)."""
    arns = aws("ecs", "list-tasks", "--cluster", cluster, "--service-name", service,
               "--desired-status", "RUNNING", region=region).get("taskArns", [])
    if not arns:
        return None
    task = aws("ecs", "describe-tasks", "--cluster", cluster, "--tasks", arns[0], region=region)["tasks"][0]
    eni = next(d["value"] for a in task["attachments"] for d in a["details"] if d["name"] == "networkInterfaceId")
    nic = aws("ec2", "describe-network-interfaces", "--network-interface-ids", eni, region=region)
    return nic["NetworkInterfaces"][0].get("Association", {}).get("PublicIp")


def print_endpoints(stack: str, region: str) -> None:
    """Show where the platform can be reached."""
    out = outputs(stack, region)
    ip = public_ip(out["ClusterName"], out["ServiceName"], region)
    print(f"\nStack {stack}: {out['_status']}")
    if not ip:
        print("No running task yet (it may be starting). Try: python deploy/capstone/aws/deploy.py status")
        return
    print(f"""
  GraphQL (GraphiQL in a browser) : http://{ip}:8080/graphql
  SSE alert stream                : curl -N http://{ip}:8080/alerts/stream
  Health                          : http://{ip}:8080/health
  gRPC ingest                     : {ip}:50051
  End-to-end smoke test           : python deploy/capstone/smoke_test.py --host {ip}
  Logs                            : aws logs tail {out['LogGroupName']} --follow --region {region}
""")


def cmd_up(a: argparse.Namespace) -> None:
    """Build, push and deploy."""
    need("docker", "https://docs.docker.com/get-started/get-docker/")
    print("1/4 Build and push the image to Amazon ECR")
    uri = build_and_push(a.region, a.repo, a.arch, a.base_image)
    print("2/4 Discover the default VPC and public subnets")
    vpc, subnets = default_network(a.region)
    role = lab_role_arn(a.region) if a.lab_role or a.lab_role is None else None
    if role:
        print(f"   using existing execution role {role}")
    params = [f"ImageUri={uri}", f"VpcId={vpc}", f"SubnetIds={','.join(subnets)}",
              f"AllowedCidr={a.allowed_cidr or my_cidr()}", f"ExecutionRoleArn={role or ''}",
              f"CpuArchitecture={'ARM64' if a.arch == 'arm64' else 'X86_64'}"]
    print("3/4 Deploy the CloudFormation stack (ECS cluster, task definition, service, security group, logs)")
    run(["aws", "cloudformation", "deploy", "--region", a.region, "--stack-name", a.stack,
         "--template-file", str(TEMPLATE), "--capabilities", "CAPABILITY_IAM", "--no-fail-on-empty-changeset",
         "--parameter-overrides", *params], capture=False)
    out = outputs(a.stack, a.region)
    print("4/4 Wait until the ECS service is stable (the task is running and healthy)")
    run(["aws", "ecs", "wait", "services-stable", "--cluster", out["ClusterName"], "--services",
         out["ServiceName"], "--region", a.region])
    print_endpoints(a.stack, a.region)


def cmd_status(a: argparse.Namespace) -> None:
    """Print stack status and endpoints."""
    print_endpoints(a.stack, a.region)


def cmd_chaos(a: argparse.Namespace) -> None:
    """Stop the running task: the ECS service (reconciliation loop) starts a replacement."""
    out = outputs(a.stack, a.region)
    arns = aws("ecs", "list-tasks", "--cluster", out["ClusterName"], "--service-name", out["ServiceName"],
               region=a.region).get("taskArns", [])
    if not arns:
        sys.exit("no running task")
    aws("ecs", "stop-task", "--cluster", out["ClusterName"], "--task", arns[0], "--reason", "chaos test",
        region=a.region)
    print("Task stopped. ECS will start a new one within a minute (new public IP; the task volume is\n"
          "ephemeral, so the read model starts empty - see README for EFS-based durability).")
    run(["aws", "ecs", "wait", "services-stable", "--cluster", out["ClusterName"], "--services",
         out["ServiceName"], "--region", a.region])
    print_endpoints(a.stack, a.region)


def cmd_down(a: argparse.Namespace) -> None:
    """Delete the stack (and optionally the ECR repository with its images)."""
    run(["aws", "cloudformation", "delete-stack", "--stack-name", a.stack, "--region", a.region])
    run(["aws", "cloudformation", "wait", "stack-delete-complete", "--stack-name", a.stack, "--region", a.region])
    if a.purge_images:
        aws("ecr", "delete-repository", "--repository-name", a.repo, "--force", region=a.region, check=False)
    print("Deleted. Nothing is running any more (no further Fargate charges).")


def main() -> None:
    """CLI entry point."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["up", "status", "chaos", "down"])
    ap.add_argument("--stack", default="weather-platform")
    ap.add_argument("--repo", default="weather-platform", help="ECR repository name")
    ap.add_argument("--region", default=None)
    ap.add_argument("--arch", choices=["amd64", "arm64"], default="amd64", help="Fargate CPU architecture")
    ap.add_argument("--allowed-cidr", default=None, help="CIDR allowed to reach the ports (default: your IP/32)")
    ap.add_argument("--lab-role", action=argparse.BooleanOptionalAction, default=None,
                    help="use the AWS Academy LabRole as execution role (default: auto-detect)")
    ap.add_argument("--base-image", default=None, help="e.g. public.ecr.aws/docker/library/python:3.12-slim")
    ap.add_argument("--purge-images", action="store_true", help="with 'down': also delete the ECR repository")
    a = ap.parse_args()
    need("aws", "https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html")
    a.region = a.region or default_region()
    print(f"Region: {a.region} · stack: {a.stack}")
    {"up": cmd_up, "status": cmd_status, "chaos": cmd_chaos, "down": cmd_down}[a.command](a)


if __name__ == "__main__":
    main()
