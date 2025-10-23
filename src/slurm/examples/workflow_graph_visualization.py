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
@task(time="00:01:00")
def load_data(dataset_name: str) -> dict:
    """Load dataset from disk."""
    print(f"Loading dataset: {dataset_name}")
    time.sleep(0.5)
    return {"dataset": dataset_name, "rows": 1000, "cols": 10}


@task(time="00:01:00")
def preprocess(data: dict) -> dict:
    """Preprocess the data."""
    print(f"Preprocessing {data['rows']} rows")
    time.sleep(0.5)
    return {**data, "preprocessed": True}


@task(time="00:02:00")
def train_model(data: dict, model_type: str) -> dict:
    """Train a model on the data."""
    print(f"Training {model_type} model")
    time.sleep(1.0)
    return {"model": model_type, "accuracy": 0.95, "data": data}


@task(time="00:01:00")
def evaluate_model(model: dict) -> dict:
    """Evaluate model performance."""
    print(f"Evaluating {model['model']} model")
    time.sleep(0.5)
    return {**model, "test_accuracy": 0.93}


@workflow(time="00:10:00")
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
    print(f"Starting ML pipeline for dataset: {dataset_name}")

    # Load and preprocess data
    data_job = load_data(dataset_name)
    preprocessed_job = preprocess(data_job)

    # Train multiple models in parallel
    model1_job = train_model(preprocessed_job, "logistic_regression")
    model2_job = train_model(preprocessed_job, "random_forest")

    # Evaluate models
    eval1_job = evaluate_model(model1_job)
    eval2_job = evaluate_model(model2_job)

    # Wait for all evaluations to complete
    results = [eval1_job.get_result(), eval2_job.get_result()]

    print(f"Pipeline complete! Trained {len(results)} models")
    return results


def main():
    """Run the workflow graph visualization example."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run workflow graph visualization example."
    )
    parser.add_argument(
        "--slurmfile",
        default=None,
        help="Path to the Slurmfile to load",
    )
    parser.add_argument(
        "--env",
        default="default",
        help="Environment key within the Slurmfile",
    )
    parser.add_argument(
        "--dataset",
        default="iris_dataset",
        help="Name of the dataset to process",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("Workflow Graph Visualization Example")
    print("=" * 70)
    configure_logging()

    # Create callback to track workflow graph
    graph_callback = WorkflowGraphCallback()
    logger_callback = LoggerCallback()

    # Create cluster from Slurmfile
    print("\nLoading cluster configuration...")
    if args.slurmfile:
        print(f"  Slurmfile: {args.slurmfile}")
    print(f"  Environment: {args.env}")
    print()

    cluster = Cluster.from_env(
        args.slurmfile,
        env=args.env,
        callbacks=[graph_callback, logger_callback],
    )

    print("Submitting ML pipeline workflow...")
    job = cluster.submit(ml_pipeline)(args.dataset)

    print(f"Workflow job submitted: {job.id}")
    print("Waiting for workflow to complete...\n")

    # Wait for completion
    result = job.get_result()

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
