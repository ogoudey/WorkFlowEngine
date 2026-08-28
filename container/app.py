"""
Workflow engine — runs inside the Fargate task.

Push me to ECR, then run the task in Fargate. The task will start a FastAPI server that listens for workflow requests. Each workflow is a list of blocks, which are executed in order. Each block is a Batch job, and the engine polls for the job status until it reaches a terminal state.

(login aws ecr get-login-password --region us-east-2 | sudo docker login --username AWS --password-stdin 538091937392.dkr.ecr.us-east-2.amazonaws.com)

docker build -t batch-submitter . --no-cache
docker tag batch-submitter:latest 538091937392.dkr.ecr.us-east-2.amazonaws.com/batch-submitter:latest
docker push 538091937392.dkr.ecr.us-east-2.amazonaws.com/batch-submitter:latest
"""
import time
from typing import Annotated, Literal, Union, reveal_type, Optional, List, Any
import uuid
import threading
from datetime import datetime, timezone
from enum import Enum
import os
import boto3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, ValidationError
import logging
import uvicorn
import dataclasses
import shlex
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="workflow-engine")
dry_run = False
try:
    batch = boto3.client("batch", os.environ.get("AWS_DEFAULT_REGION", "us-east-2"))
except Exception as e:
    batch = None
    logger.error(f"Error initializing Batch client: {e}. Dev mode?")

# ---- in-memory state -------------------------------------------------
# workflow_id -> workflow dict. Fine for now; swap for DynamoDB later
# without touching the API surface below.
LOCK = threading.Lock()

POLL_INTERVAL_SECONDS = 15
TERMINAL_STATES = ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]
current_block = None

# ---- commands to give to instance ---------------------------------------------------------------


class BlockCommand(BaseModel):
    def to_list(self) -> list[str]:
        """
        Convert the instance variables to a list of arguments for the Batch job command. The order of the arguments is determined by the order of the fields in the model.
        """
        return [str(v) for v in self.model_dump().values()]

    def to_sh(self) -> List[str]:
        if dataclasses.is_dataclass(self):
            values = {
                f.name: getattr(self, f.name) for f in dataclasses.fields(self)
            }
        else:
            values = {
                k: v
                for k, v in vars(self).items()
                if not k.startswith("_") and k != "commands"
            }

        formatted_sequences = []

        for sequence in self.commands:
            cmd_args = []
            i = 0
            while i < len(sequence):
                token = sequence[i]

                # Check if this token is a flag paired with a placeholder (e.g., ["--delete_originals", "{delete_originals}"])
                if (
                    token.startswith("--")
                    and i + 1 < len(sequence)
                    and sequence[i + 1].startswith("{")
                ):
                    var_name = sequence[i + 1][1:-1]
                    val = values.get(var_name)

                    if isinstance(val, bool):
                        if val:
                            cmd_args.append(
                                token
                            )  # Append flag only if True (omit value)
                        i += 2
                        continue
                    elif val is not None:
                        cmd_args.extend([token, str(val)])  # Append flag + value
                        i += 2
                        continue
                    else:
                        i += 2  # Skip both if None
                        continue

                # Standalone placeholder handling (e.g. positional arg "{tmp_source}")
                if token.startswith("{") and token.endswith("}"):
                    var_name = token[1:-1]
                    val = values.get(var_name)

                    if isinstance(val, bool):
                        if val:
                            # Fallback for standalone boolean placeholders without an explicit '--flag'
                            cmd_args.append(f"--{var_name}")
                    elif val is not None:
                        cmd_args.append(str(val))

                else:
                    cmd_args.append(token)

                i += 1

            quoted_cmd = " ".join(shlex.quote(a) for a in cmd_args)
            formatted_sequences.append(quoted_cmd)

        full_command = " && ".join(formatted_sequences)
        return ["sh", "-c", full_command] 

class TestBlockCommand(BlockCommand):
    sys0: str = "echo"
    sys1: str = "hello world"

class TestAWSCommand(BlockCommand):
    sys0: str = "aws"
    sys1: str = "s3"
    sys2: str = "ls"

class SyncS3BucketCommand(BlockCommand):
    sys0: str = "aws"
    sys1: str = "s3"
    sys2: str = "sync"
    source_bucket: str
    destination: str = "tmp/data"

class TestLongBlockCommand(BlockCommand):
    commands: List[List[str]] = [
        [
            "aws",
            "s3",
            "sync",
            "{source_bucket}",
            "{tmp_destination}",
        ],
        [
            "echo",
            "Hello, World!"
        ],
        [
            "aws",
            "s3",
            "sync",
            "{tmp_destination}",
            "{destination}",
            "--delete"
        ],
    ]
    source_bucket: str
    tmp_destination: str = "tmp/data"
    destination: str

class LeRobotConversionCommand(BlockCommand):
    commands: List[List[str]] = [
        [
            "aws",
            "s3",
            "sync",
            "{source_bucket}",
            "{tmp_source}",
        ],
        [
            "python3",
            "main.py",
            "{tmp_source}",
            "{tmp_destination}",
            "{in_expectation}",
            "{out_format}"
        ],
        [
            "aws",
            "s3",
            "sync",
            "{tmp_destination}",
            "{destination}",
            "--delete"
        ],
    ]
    source_bucket: str
    destination: str
    in_expectation: str
    out_format: str
    tmp_source: str = "tmp/input_dataset"
    tmp_destination: str = "tmp/output_dataset"

class SyncTo30Command(BlockCommand):
    commands: List[List[str]] = [
        [
            "ls"
        ],
        [
            "aws",
            "s3",
            "sync",
            "{source_bucket}",
            "{tmp_source}",
        ],
        [
            "python3",
            "sync_to_30.py",
            "{tmp_source}",
            "{delete_originals}"
        ],
        [
            "aws",
            "s3",
            "sync",
            "{tmp_source}",
            "{destination}",
            "--delete"
        ],
    ]
    source_bucket: str
    destination: str
    delete_originals: bool = True
    tmp_source: str = "tmp/input_dataset"

class CompositeVideosCommand(BlockCommand):
    commands: List[List[str]] = [
        [
            "aws",
            "s3",
            "sync",
            "{source_bucket}",
            "{tmp_source}",
        ],
        [
            "python3",
            "composite.py",
            "{tmp_source}",
            "{delete_originals}"
        ],
        [
            "aws",
            "s3",
            "sync",
            "{tmp_source}",
            "{destination}",
            "--delete"
        ],
    ]
    source_bucket: str
    destination: str
    delete_originals: bool = False
    tmp_source: str = "tmp/input_dataset"

class HandRemovalCommand(BlockCommand):
    sys0: str = "python3"
    sys1: str = "remove_hands.py"
    source_bucket: str
    destination_bucket: str

class IsaacFinetuneCommand(BlockCommand):
    commands: List[List[str]] = [
        [
            "aws",
            "s3",
            "sync",
            "{source_bucket}",
            "{tmp_source}",
        ],
        [
            "python3",
            "finetune.py",
            "--source",
            "{tmp_source}",
            "--output",
            "{tmp_dest}",
            "--steps",
            "{steps}",
        ],
        [
            "aws",
            "s3",
            "sync",
            "{tmp_dest}",
            "{destination}",
            "--delete"
        ],
    ]

    source_bucket: str
    destination: str
    steps: int = 20000
    dryrun: bool = False
    tmp_source: str = "tmp/input_dataset"
    tmp_dest: str = "tmp/output"

    

# ---- batch job block requests ---------------------------------------------------------------

class TestLocalBlockRequest(BaseModel):
    name: Literal["test_local_block"]

class GetModalityRequest(BaseModel):
    name: Literal["get_modality"]
    robot_type: str

class TestBlockRequest(BaseModel):
    name: Literal["test_block"]
    job_queue: str = "minimal_queue"
    job_definition: str = "TestBlockCommand"
    environment: List[dict] = Field(default_factory=list)
    command: TestBlockCommand = Field(default_factory=TestBlockCommand)

class TestAWSRequest(BaseModel):
    name: Literal["test_aws_block"]
    job_queue: str = "minimal_queue"
    job_definition: str = "TestAWSCommand"
    environment: List[dict] = Field(default_factory=list)
    command: TestAWSCommand = Field(default_factory=TestAWSCommand)

class TestLongBlockRequest(BaseModel):
    name: Literal["test_long_block"]
    job_queue: str = "minimal_queue"
    job_definition: str = "TestLongBlockCommand"
    environment: List[dict] = Field(default_factory=list)
    command: TestLongBlockCommand = Field(default_factory=TestLongBlockCommand)

class SyncS3BucketRequest(BaseModel):
    name: Literal["sync_s3_bucket"]
    job_queue: str = "minimal_queue"
    job_definition: str = "SyncS3BucketCommand"
    environment: List[dict] = Field(default_factory=list)
    command: SyncS3BucketCommand = Field(default_factory=SyncS3BucketCommand)

class HandRemovalRequest(BaseModel):
    name: Literal["hand_removal"]
    job_queue: str = "minimal_queue"
    job_definition: str = "run_block_job"
    environment: List[dict] = Field(default_factory=list)
    command: HandRemovalCommand = Field(default_factory=HandRemovalCommand)

class LeRobotAdapterRequest(BaseModel):
    name: Literal["lerobot_adapter"]
    job_queue: str = "medium_queue"
    job_definition: str = "!TBD! Depends on command out_format"
    environment: List[dict] = Field(default_factory=list)
    command: LeRobotConversionCommand = Field(default_factory=LeRobotConversionCommand)

class SyncTo30Request(BaseModel):
    name: Literal["sync_to_30"]
    job_queue: str = "small_queue"
    job_definition: str = "SyncTo30Command"
    environment: List[dict] = Field(default_factory=list)
    command: SyncTo30Command = Field(default_factory=SyncTo30Command)

class CompositeVideosRequest(BaseModel):
    name: Literal["composite_videos"]
    job_queue: str = "small_queue"
    job_definition: str = "CompositeVideosCommand"
    environment: List[dict] = Field(default_factory=list)
    command: CompositeVideosCommand = Field(default_factory=CompositeVideosCommand)

class IsaacFinetuneRequest(BaseModel):
    name: Literal["finetune_groot"]
    job_queue: str = "minimal_queue"
    job_definition: str = "run_block_job"
    environment: List[dict] = Field(default_factory=list)
    command: IsaacFinetuneCommand = Field(default_factory=IsaacFinetuneCommand)

BlockRequest = Annotated[
    Union[TestBlockRequest, TestAWSRequest, TestLongBlockRequest, SyncS3BucketRequest, LeRobotAdapterRequest, IsaacFinetuneRequest, SyncTo30Request, CompositeVideosRequest],
    Field(discriminator="name"),
]

class WorkflowRequest(BaseModel):
    workflow_id: str
    blocks: list[BlockRequest]

# ---- main ---------------------------------------------------------------

def main_start_workflow() -> Optional[threading.Thread]:
    """
    Return None if no workflow is started - i.e. dev mode
    """
    raw_env_json = os.environ.get("WORKFLOW_REQUEST")
    if not raw_env_json:
        logger.error("WORKFLOW_REQUEST env var not set. Not starting any workflow. Use /workflows to start one.")
        return
    try:
        workflow = WorkflowRequest.model_validate_json(raw_env_json)
        
        logger.info(f"Successfully loaded workflow: {workflow.workflow_id}")
        logger.info(f"Workflow has {len(workflow.blocks)} blocks:")
        for block in workflow.blocks:
            logger.info(f" - Found block: {block}")

                
    except ValidationError as e:
        # Catches JSON syntax errors AND validation mismatches (e.g., bad discriminator name)
        logger.error(f"Failed to parse WORKFLOW_REQUEST object: {e}")
        return

    thread = threading.Thread(
        target=_run_workflow, args=(workflow.workflow_id, workflow.blocks), daemon=True
    )
    thread.start()
    return thread

    

# ---- API ---------------------------------------------------------------

@app.post("/workflows")
def start_workflow(req: WorkflowRequest):
    if not req.workflow_id:
        raise HTTPException(400, "workflow_id is required")
    
    workflow_id = req.workflow_id
    if not req.blocks:
        raise HTTPException(400, "workflow must have at least one step")

    thread = threading.Thread(
        target=_run_workflow, args=(workflow_id, req.blocks), daemon=True
    )
    thread.start()

    return {"workflow_id": workflow_id}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/workflows/{workflow_id}")
def get_current_block(workflow_id: str):
    global current_block
    print(f"current block for {workflow_id} is {current_block}")
    if current_block is None:
        raise HTTPException(404, "workflow not running")
    return current_block

# ---- engine internals ---------------------------------------------------

def _run_workflow(workflow_id: str, blocks: list[BlockRequest]):
    global current_block
    if not batch:
        logger.error("Batch client not initialized. Running in mock.")
    while len(blocks) > 0:
        logger.info(f"Popping block from workflow {workflow_id}: {blocks[0]}")
        
        current_block = blocks.pop(0)

        terminal_status =_block_handler(current_block)
        if terminal_status == "SUCCEEDED":
            continue
        if terminal_status != "SUCCEEDED":
            logger.error(f"Workflow {workflow_id} died at block {current_block.name} with status {terminal_status}")
            return
    current_block = None
    logger.info(f"Workflow {workflow_id} complete")

def _block_handler(block: BlockRequest) -> str:
    if block.name == "test_block":
        job_id = _submit_batch_job(block)
        return _poll_until_done(job_id, mock_time=1)
    elif block.name == "test_local_block":
        return _local_block(block)
    elif block.name == "get_modality":
        return _local_block(block)
    elif block.name == "test_long_block":
        job_id = _submit_batch_job(block)
        return _poll_until_done(job_id, mock_time=60)
    elif block.name == "sync_s3_bucket":
        job_id = _submit_batch_job(block)
        return _poll_until_done(job_id)
    elif block.name == "lerobot_adapter":
        print(f"This block forks containers {block.command}.")
        block.job_definition = "LeRobotV2ConversionJob" if "v21" in block.command.out_format else "LeRobotV3ConversionJob"     
        job_id = _submit_batch_job(block)
        return _poll_until_done(job_id)
    elif block.name == "sync_to_30":
        job_id = _submit_batch_job(block)
        return _poll_until_done(job_id)
    elif block.name == "composite_videos":
        job_id = _submit_batch_job(block)
        return _poll_until_done(job_id)
    elif block.name == "hand_removal":
        job_id = _submit_batch_job(block)
        return _poll_until_done(job_id)
    elif block.name == "finetune_groot":
        job_id = _submit_batch_job(block)
        return _poll_until_done(job_id)
    else:
        logger.error(f"Unknown block type: {block.name}")
        return "FAILED"

def _submit_batch_job(block: BlockRequest) -> str:
    logger.info(f"Submitting {block.name}")
    logger.info(f"More info: {block}")
    logger.info(f"More info: {block.model_dump()}")
    logger.info(f"Block Request args: {block} \n        {{\n            \"command\": {block.command.to_sh() if hasattr(block.command, "commands") else block.command.to_list()},\n            \"environment\": {block.environment}\n        }}")
    if not batch:
        time.sleep(1)
        return "0"
    global dry_run
    if dry_run:
        return "0"
    response = batch.submit_job(
        jobName=f"{block.name}",
        jobQueue=f"{block.job_queue}",
        jobDefinition=f"{block.job_definition}",
        containerOverrides={
            "command": block.command.to_sh() if hasattr(block.command, "commands") else block.command.to_list(),
            "environment": block.environment
        }
    )
    logger.info(f"Submitted {block.name} with {response["jobId"]}")
    return response["jobId"]

def _local_block(block: BlockRequest):
    logger.info(f"Running local job for {block.name}:\n{block.model_dump()}")
    return _submit_local_job(block)

def _submit_local_job(block):
    import local_jobs # don't import globally, no need.

    if block.name == "test_local_block":
        return "SUCCEEDED"
    elif block.name == "get_modality":
        local_jobs.get_modality(block.robot_type)
    elif block.name == "composite_videos":
        local_jobs.composite_videos(block.video, block.overlay)
    else:
        logger.error(f"Local block {block.name} not supported.")
        return "FAILED"

def _poll_until_done(job_id: str, mock_time: float = 1.0) -> str:
    if not batch:
        time.sleep(mock_time)
        logger.info(f"Job {job_id} status: SUCCEEDED (mock)")
        return "SUCCEEDED"
    try:
        while True:
        
            resp = batch.describe_jobs(jobs=[job_id])
            job = resp["jobs"][0]
            status = job["status"]
            logger.info(f"Job {job_id} status: {status}")
            if status in TERMINAL_STATES:
                return status
            time.sleep(POLL_INTERVAL_SECONDS)
    except Exception as e:
        logger.error(f"Error polling job {job_id}: {e}. We'll just sleep 3 seconds.")
        time.sleep(3)
        return "SUCCEEDED"

if __name__ == "__main__":
    import sys
    if sys.argv[1] == "dry-run":
        dry_run = True
    workflow_thread = main_start_workflow()
   
    if workflow_thread: # If the workflow request is good, let it finish and kill the API.
        config = uvicorn.Config(app, host="0.0.0.0", port=8000)
        server = uvicorn.Server(config)
        api_thread = threading.Thread(target=server.run, daemon=True)
        api_thread.start()
        workflow_thread.join()
        server.should_exit = True
        api_thread.join()
    else: # Otherwise, let the API run, and the user can start a workflow via the API.
        uvicorn.run(app, host="0.0.0.0", port=8000) 
    
    logger.info("bye")
