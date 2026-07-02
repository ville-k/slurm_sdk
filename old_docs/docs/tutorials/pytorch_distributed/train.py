"""Distributed PyTorch training task that works locally and on SLURM clusters."""

import argparse
import logging
import os
from typing import Optional

from slurm.callbacks.callbacks import LoggerCallback
import torch
import torch.nn as nn
import torch.optim as optim
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader, DistributedSampler
from torchvision import datasets, transforms

from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.runtime import JobContext


def get_device() -> str:
    """Auto-detect best available device."""
    if torch.cuda.is_available():
        return "cuda"
    elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return "mps"
    else:
        return "cpu"


def setup_distributed_pytorch(job: JobContext) -> None:
    """Setup PyTorch distributed environment from SLURM job context."""
    import torch.distributed as dist

    # Get distributed environment variables from JobContext
    env = job.torch_distributed_env()
    os.environ.update(env)

    # Initialize process group
    dist.init_process_group(backend="nccl" if torch.cuda.is_available() else "gloo")

    # Set device for this process
    if torch.cuda.is_available():
        local_rank = int(os.environ["LOCAL_RANK"])
        torch.cuda.set_device(local_rank)


class SimpleCNN(nn.Module):
    """Simple CNN for MNIST classification."""

    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 64, 3, 1)
        self.dropout1 = nn.Dropout(0.25)
        self.dropout2 = nn.Dropout(0.5)
        self.fc1 = nn.Linear(9216, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, x):
        x = self.conv1(x)
        x = nn.functional.relu(x)
        x = self.conv2(x)
        x = nn.functional.relu(x)
        x = nn.functional.max_pool2d(x, 2)
        x = self.dropout1(x)
        x = torch.flatten(x, 1)
        x = self.fc1(x)
        x = nn.functional.relu(x)
        x = self.dropout2(x)
        x = self.fc2(x)
        return nn.functional.log_softmax(x, dim=1)


def get_data_loaders(batch_size: int, distributed: bool = False):
    """Create train and test data loaders."""
    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
    )

    train_dataset = datasets.MNIST(
        "./data", train=True, download=True, transform=transform
    )
    test_dataset = datasets.MNIST("./data", train=False, transform=transform)

    if distributed:
        train_sampler = DistributedSampler(train_dataset)
        test_sampler = DistributedSampler(test_dataset, shuffle=False)
    else:
        train_sampler = None
        test_sampler = None

    train_loader = DataLoader(
        train_dataset,
        batch_size=batch_size,
        sampler=train_sampler,
        shuffle=(train_sampler is None),
    )
    test_loader = DataLoader(
        test_dataset,
        batch_size=batch_size,
        sampler=test_sampler,
        shuffle=False,
    )

    return train_loader, test_loader


def train_epoch(model, device, train_loader, optimizer, epoch, rank):
    """Train for one epoch."""
    model.train()
    total_loss = 0
    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = nn.functional.nll_loss(output, target)
        loss.backward()
        optimizer.step()

        total_loss += loss.item()

        if batch_idx % 100 == 0 and rank == 0:
            print(
                f"Epoch {epoch} [{batch_idx}/{len(train_loader)}] Loss: {loss.item():.6f}"
            )

    avg_loss = total_loss / len(train_loader)
    return avg_loss


def test(model, device, test_loader, rank):
    """Evaluate model on test set."""
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)
            test_loss += nn.functional.nll_loss(output, target, reduction="sum").item()
            pred = output.argmax(dim=1, keepdim=True)
            correct += pred.eq(target.view_as(pred)).sum().item()

    test_loss /= len(test_loader.dataset)
    accuracy = 100.0 * correct / len(test_loader.dataset)

    if rank == 0:
        print(
            f"\nTest set: Average loss: {test_loss:.4f}, Accuracy: {correct}/{len(test_loader.dataset)} ({accuracy:.2f}%)\n"
        )

    return test_loss, accuracy


@task(
    time="01:00:00",
    ntasks=8,
    gpus_per_node=8,
    nodes=1,
    mem="8G",
    account="av_alpamayo_training",
    partition="interactive_singlenode",
)
def train_distributed(
    epochs: int = 5,
    batch_size: int = 64,
    lr: float = 0.001,
    *,
    job: Optional[JobContext] = None,
) -> dict:
    """Train MNIST classifier with optional distributed setup.

    Args:
        epochs: Number of training epochs
        batch_size: Batch size per process
        lr: Learning rate
        job: Optional JobContext (injected by SDK when running on cluster)

    Returns:
        dict with training results
    """
    # Auto-detect device
    device = get_device()

    # Setup distributed training if on cluster
    distributed = False
    rank = 0
    world_size = 1

    if job is not None:
        # We're on a SLURM cluster with multiple tasks
        setup_distributed_pytorch(job)
        rank = int(os.environ["RANK"])
        world_size = int(os.environ["WORLD_SIZE"])
        distributed = True

        if rank == 0:
            print(f"Running distributed training on {world_size} processes")
            print(f"Job ID: {job.job_id}")
            print(f"Nodes: {job.hostnames}")
            print(f"Output dir: {job.output_dir}")

    if rank == 0:
        print(f"Device: {device}")
        print(f"Epochs: {epochs}, Batch size: {batch_size}, LR: {lr}")

    # Create model
    model = SimpleCNN().to(device)

    if distributed:
        # Wrap model in DDP for distributed training
        if device == "cuda":
            local_rank = int(os.environ["LOCAL_RANK"])
            model = DDP(model, device_ids=[local_rank])
        else:
            model = DDP(model)

    # Create optimizer
    optimizer = optim.Adam(model.parameters(), lr=lr)

    # Get data loaders
    train_loader, test_loader = get_data_loaders(batch_size, distributed)

    # Training loop
    results = {"epochs": [], "test_accuracy": None}

    for epoch in range(1, epochs + 1):
        if distributed:
            # Ensure different shuffling for each epoch
            train_loader.sampler.set_epoch(epoch)

        train_loss = train_epoch(model, device, train_loader, optimizer, epoch, rank)

        if rank == 0:
            results["epochs"].append({"epoch": epoch, "train_loss": train_loss})

    # Final evaluation
    test_loss, test_accuracy = test(model, device, test_loader, rank)

    if rank == 0:
        results["test_loss"] = test_loss
        results["test_accuracy"] = test_accuracy

        # Save model checkpoint if output dir is available
        if job and job.output_dir:
            checkpoint_path = job.output_dir / "model.pt"
            torch.save(model.state_dict(), checkpoint_path)
            print(f"Saved model to {checkpoint_path}")
            results["checkpoint_path"] = str(checkpoint_path)

    # Cleanup distributed
    if distributed:
        import torch.distributed as dist

        dist.destroy_process_group()

    return results


def main():
    """Main entry point for local and cluster execution."""
    parser = argparse.ArgumentParser(description="PyTorch distributed training")
    parser.add_argument("--epochs", type=int, default=5, help="Number of epochs")
    parser.add_argument("--batch-size", type=int, default=64, help="Batch size")
    parser.add_argument("--lr", type=float, default=0.001, help="Learning rate")
    parser.add_argument("--submit", action="store_true", help="Submit to SLURM cluster")
    parser.add_argument(
        "--env", type=str, default="local", help="Slurmfile environment"
    )
    parser.add_argument(
        "--slurmfile", type=str, default="Slurmfile.toml", help="Path to Slurmfile"
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    if args.submit:
        # Submit to cluster
        print(f"Submitting job to cluster (env={args.env})")

        cluster = Cluster.from_env(
            args.slurmfile, env=args.env, callbacks=[LoggerCallback()]
        )

        job = cluster.submit(train_distributed)(
            epochs=args.epochs,
            batch_size=args.batch_size,
            lr=args.lr,
        )

        print(f"Job submitted: {job.id}")
        print(f"Job directory: {job.target_job_dir}")
        print("Waiting for job to complete...")

        success = job.wait(timeout=3600, poll_interval=10)

        if success:
            result = job.get_result()
            print("\n=== Training completed successfully ===")
            print(f"Test accuracy: {result['test_accuracy']:.2f}%")
            if "checkpoint_path" in result:
                print(f"Model saved to: {result['checkpoint_path']}")
        else:
            print("\nJob failed or timed out")
            print(f"Status: {job.get_status()}")

    else:
        # Run locally
        print("Running locally (single process)")
        result = train_distributed(
            epochs=args.epochs,
            batch_size=args.batch_size,
            lr=args.lr,
        )
        print("\n=== Training completed ===")
        print(f"Test accuracy: {result['test_accuracy']:.2f}%")


if __name__ == "__main__":
    main()
