import time
import uuid

from neptune_scale import Run

project = "patryk.gala/test"
if __name__ == "__main__":
    start = time.time()
    run = Run(project=project, run_id=f"{uuid.uuid4()}", experiment_name="test")

    run.log_configs(
        data={
            "parameters/learning_rate": 0.001,
            "parameters/batch_size": 64,
        },
    )
    for step in range(10):
        metrics = {f"metric{i}": step + i for i in range(100)}
        run.log_metrics(
            data=metrics,
            step=step,
        )
    run.wait_for_submission()
    print(f"Time: {time.time() - start}s")
