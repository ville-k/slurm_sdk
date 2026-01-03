"""Example: Visualize workflow dependency graphs using callbacks.

This example demonstrates how to use callbacks to build and export workflow
dependency graphs for visualization with Graphviz.

The WorkflowGraphCallback tracks workflow orchestration and child task
submissions, building a directed graph that shows the structure of your
computational workflow.

Usage:
    # Using default environment from Slurmfile
    python -m slurm.examples.workflow_graph_visualization
    
    # Specifying custom Slurmfile and environment
    python -m slurm.examples.workflow_graph_visualization \\
        --slurmfile Slurmfile.toml --env oci-container

Output:
    - Prints workflow execution info
    - Creates workflow_graph.dot file (Graphviz format)
    - Creates workflow_graph.json file (JSON format)

To visualize the DOT file:
    dot -Tpng workflow_graph.dot -o workflow_graph.png
"""

import argparse
import json
import time
from pathlib import Path
from typing import Dict, List

from slurm import Cluster, task
from slurm.logging import configure_logging
from slurm.decorators import workflow
from slurm.workflow import WorkflowContext
from slurm.callbacks import (
    BaseCallback,
    LoggerCallback,
    WorkflowCallbackContext,
    WorkflowTaskSubmitContext,
    SubmitBeginContext,
    SubmitEndContext,
)


class WorkflowGraphCallback(BaseCallback):
    """Build workflow dependency graph for visualization.

    This callback constructs a directed graph of workflow task dependencies
    that can be exported to various formats (DOT, JSON, etc.) for visualization.

    Note: This callback tracks tasks submitted directly from the client, but
    child tasks submitted from within workflows (which run on the cluster) are
    not captured unless the callback is picklable and transported to the cluster.

    For full workflow graph tracking including child tasks, the callback would
    need to be picklable (requires_pickling=True) and handle distributed state
    management.
    """

    # This callback only runs on the client side
    requires_pickling = False

    # Override execution loci to run on CLIENT only
    execution_loci = {
        "on_workflow_begin_ctx": "client",
        "on_workflow_end_ctx": "client",
    }

    def __init__(self):
        """Initialize the graph builder."""
        super().__init__()
        self.graph: Dict[str, Dict] = {}
        self.submission_order: List[str] = []

    def on_begin_submit_job_ctx(self, ctx: SubmitBeginContext):
        """Track when workflows/tasks are about to be submitted from the client."""
        # Check if this is a workflow submission
        if hasattr(ctx.task, "_is_workflow") and ctx.task._is_workflow:
            # We'll use the pre_submission_id temporarily and update with actual job_id later
            self.graph[ctx.pre_submission_id] = {
                "name": ctx.task.func.__name__,
                "type": "workflow",
                "children": [],
                "start_time": ctx.timestamp,
                "pre_submission_id": ctx.pre_submission_id,
            }
            self.submission_order.append(ctx.pre_submission_id)

    def on_end_submit_job_ctx(self, ctx: SubmitEndContext):
        """Update graph with actual job IDs after submission."""
        # Check if we tracked this submission by pre_submission_id
        if ctx.pre_submission_id in self.graph:
            # Move the entry from pre_submission_id to actual job_id
            entry = self.graph.pop(ctx.pre_submission_id)
            entry.pop("pre_submission_id", None)  # Remove temporary field
            self.graph[ctx.job_id] = entry
            # Update submission order
            idx = self.submission_order.index(ctx.pre_submission_id)
            self.submission_order[idx] = ctx.job_id

    def on_workflow_begin_ctx(self, ctx: WorkflowCallbackContext):
        """Called when workflow orchestrator starts execution."""
        # This now runs on client if execution_loci is set correctly
        if ctx.workflow_job_id not in self.graph:
            self.graph[ctx.workflow_job_id] = {
                "name": ctx.workflow_name,
                "type": "workflow",
                "children": [],
                "start_time": ctx.timestamp,
            }
            self.submission_order.append(ctx.workflow_job_id)

    def on_workflow_task_submitted_ctx(self, ctx: WorkflowTaskSubmitContext):
        """Called when workflow submits a child task."""
        if ctx.parent_workflow_id in self.graph:
            self.graph[ctx.parent_workflow_id]["children"].append(
                {
                    "job_id": ctx.child_job_id,
                    "name": ctx.child_task_name,
                    "is_workflow": ctx.child_is_workflow,
                }
            )

            # Add child node if it's not already tracked
            if ctx.child_job_id not in self.graph:
                self.graph[ctx.child_job_id] = {
                    "name": ctx.child_task_name,
                    "type": "workflow" if ctx.child_is_workflow else "task",
                    "children": [],
                    "parent": ctx.parent_workflow_id,
                }
                self.submission_order.append(ctx.child_job_id)

    def export_dot(self) -> str:
        """Export graph in Graphviz DOT format.

        Returns:
            DOT format string that can be rendered with Graphviz
        """
        lines = ["digraph workflow {"]
        lines.append("  rankdir=TB;  // Top to bottom layout")
        lines.append("  node [style=filled];")
        lines.append("")

        # Define nodes
        for job_id, data in self.graph.items():
            if data["type"] == "workflow":
                shape = "box"
                fillcolor = "lightblue"
            else:
                shape = "ellipse"
                fillcolor = "lightgray"

            lines.append(
                f'  "{job_id}" [label="{data["name"]}" '
                f"shape={shape} fillcolor={fillcolor}];"
            )

        lines.append("")

        # Define edges (parent -> child relationships)
        for job_id, data in self.graph.items():
            for child in data["children"]:
                lines.append(f'  "{job_id}" -> "{child["job_id"]}";')

        lines.append("}")
        return "\n".join(lines)

    def export_json(self) -> str:
        """Export graph in JSON format.

        Returns:
            JSON string representation of the graph
        """
        return json.dumps(
            {
                "graph": self.graph,
                "submission_order": self.submission_order,
            },
            indent=2,
        )

    def get_workflow_roots(self) -> List[str]:
        """Get list of root workflow job IDs.

        Returns:
            List of job IDs for root workflows (not submitted by other workflows)
        """
        roots = []
        for job_id, data in self.graph.items():
            if data["type"] == "workflow" and "parent" not in data:
                roots.append(job_id)
        return roots


# Define a simple ML pipeline workflow
@task(time="00:03:00", ntasks=1, cpus_per_task=1, mem="256M")
def load_data(dataset_name: str) -> dict:
    """Load dataset from disk."""
    print(f"Loading dataset: {dataset_name}")
    time.sleep(0.5)
    return {"dataset": dataset_name, "rows": 1000, "cols": 10}


@task(time="00:03:00", ntasks=1, cpus_per_task=1, mem="256M")
def preprocess(data: dict) -> dict:
    """Preprocess the data."""
    print(f"Preprocessing {data['rows']} rows")
    time.sleep(0.5)
    return {**data, "preprocessed": True}


@task(time="00:03:00", ntasks=1, cpus_per_task=1, mem="256M")
def train_model(data: dict, model_type: str) -> dict:
    """Train a model on the data."""
    print(f"Training {model_type} model")
    time.sleep(1.0)
    return {"model": model_type, "accuracy": 0.95, "data": data}


@task(time="00:03:00", ntasks=1, cpus_per_task=1, mem="256M")
def evaluate_model(model: dict) -> dict:
    """Evaluate model performance."""
    print(f"Evaluating {model['model']} model")
    time.sleep(0.5)
    return {**model, "test_accuracy": 0.93}


@workflow(time="00:05:00", ntasks=1, cpus_per_task=1, mem="512M")
def ml_pipeline(dataset_name: str, ctx: WorkflowContext):
    """Complete ML pipeline from data loading to evaluation.

    This workflow demonstrates a typical machine learning workflow:
    1. Load data
    2. Preprocess
    3. Train multiple models in parallel
    4. Evaluate each model

    Args:
        dataset_name: Name of the dataset to process
        ctx: WorkflowContext (automatically injected)
    """
    import sys

    print(f"Starting ML pipeline for dataset: {dataset_name}", flush=True)
    sys.stdout.flush()
    sys.stderr.flush()

    # Step 1: Load data
    data_job = load_data(dataset_name)
    data = data_job.get_result()
    print(f"Data loaded: {data}")

    # Step 2: Preprocess
    preprocessed_job = preprocess(data)
    preprocessed = preprocessed_job.get_result()
    print(f"Data preprocessed: {preprocessed}")

    # Step 3: Train models in parallel
    model1_job = train_model(preprocessed, "logistic_regression")
    model1 = model1_job.get_result()
    model2_job = train_model(preprocessed, "random_forest")
    model2 = model2_job.get_result()

    # Step 4: Evaluate models
    print(f"Models trained: {model1['model']}, {model2['model']}")

    eval1_job = evaluate_model(model1)
    result1 = eval1_job.get_result()
    eval2_job = evaluate_model(model2)
    result2 = eval2_job.get_result()

    print(f"Pipeline complete! Results: {result1}, {result2}", flush=True)
    sys.stdout.flush()
    sys.stderr.flush()

    return {"model1": result1, "model2": result2}


def main():
    """Run the workflow graph visualization example."""
    parser = argparse.ArgumentParser(
        description="Run workflow graph visualization example."
    )

    # Add standard cluster configuration arguments
    Cluster.add_argparse_args(parser)

    # Add example-specific arguments
    parser.add_argument(
        "--dataset",
        default="iris_dataset",
        help="Name of the dataset to process",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Timeout in seconds for waiting for workflow completion (default: 300)",
    )

    args = parser.parse_args()

    print("=" * 70)
    print("Workflow Graph Visualization Example")
    print("=" * 70)
    configure_logging()

    # Create callback to track workflow graph
    graph_callback = WorkflowGraphCallback()
    logger_callback = LoggerCallback()

    # Build cluster kwargs
    cluster_kwargs = {
        "callbacks": [logger_callback, graph_callback],
    }

    # Only set default packaging/dockerfile if the user didn't specify packaging via --packaging
    # This allows tests to specify packaging="none" or "wheel" without building containers
    packaging_arg = getattr(args, "packaging", None)
    if packaging_arg is None:
        # No --packaging argument provided, use container as default
        cluster_kwargs["default_packaging"] = "container"
        cluster_kwargs["default_packaging_dockerfile"] = (
            "src/slurm/examples/workflow_graph_visualization.Dockerfile"
        )
    elif packaging_arg == "container":
        # Container packaging specified, but no Dockerfile set yet - set it
        if not getattr(args, "packaging_dockerfile", None):
            cluster_kwargs["default_packaging_dockerfile"] = (
                "src/slurm/examples/workflow_graph_visualization.Dockerfile"
            )
    # If --packaging was provided with another value (wheel, none), don't set defaults

    cluster = Cluster.from_args(args, **cluster_kwargs)

    print("\nSubmitting ML pipeline workflow...")
    job = cluster.submit(ml_pipeline)(args.dataset)

    print(f"Workflow job submitted: {job.id}")
    print(f"Waiting for workflow to complete (timeout={args.timeout}s)...\n")

    # Wait for completion with configurable timeout
    try:
        result = job.get_result(timeout=args.timeout)
    except Exception as e:
        print(f"ERROR: Job timed out or failed: {e}")
        print(f"Job ID: {job.id}")
        print(f"Status: {job.get_status()}")
        try:
            print("\n--- Remote Stdout ---")
            print(job.get_stdout())
        except Exception as stdout_exc:
            print(f"\n--- Remote Stdout Unavailable: {stdout_exc} ---")
        try:
            print("\n--- Remote Stderr ---")
            print(job.get_stderr())
        except Exception as stderr_exc:
            print(f"\n--- Remote Stderr Unavailable: {stderr_exc} ---")
        raise

    print("\n" + "=" * 70)
    print("Workflow completed!")
    print("=" * 70)
    print(f"\nResults: {result}\n")

    # Export graph to DOT format
    dot_output = graph_callback.export_dot()
    dot_path = Path("workflow_graph.dot")
    dot_path.write_text(dot_output)
    print(f"Graph exported to: {dot_path.absolute()}")
    print("To visualize: dot -Tpng workflow_graph.dot -o workflow_graph.png\n")

    # Export graph to JSON format
    json_output = graph_callback.export_json()
    json_path = Path("workflow_graph.json")
    json_path.write_text(json_output)
    print(f"Graph data exported to: {json_path.absolute()}\n")

    # Print graph summary
    roots = graph_callback.get_workflow_roots()
    print("Graph Summary:")
    print(f"  Root workflows: {len(roots)}")
    print(f"  Total nodes: {len(graph_callback.graph)}")
    print(f"  Submission order: {len(graph_callback.submission_order)}")

    # Print graph structure
    print("\nGraph Structure:")
    for job_id, data in graph_callback.graph.items():
        node_type = "Workflow" if data["type"] == "workflow" else "Task"
        print(f"  {node_type}: {data['name']} (id={job_id})")
        if data["children"]:
            for child in data["children"]:
                child_type = "workflow" if child["is_workflow"] else "task"
                print(f"    └─> {child_type}: {child['name']} (id={child['job_id']})")

    print("\n" + "=" * 70)
    print("Example complete! Check the generated files:")
    print(f"  - {dot_path.absolute()}")
    print(f"  - {json_path.absolute()}")
    print("=" * 70)


if __name__ == "__main__":
    main()
