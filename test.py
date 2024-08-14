import datetime
import random
import string
import time

from neptune_scale import Run

METRIC_PREFIX = "test/"
METRIC_NUMBER_LEN = 10

number_of_metrics_per_epoch = 400_000
metric_name_len = 40
experiment_name = "exp1"


def generate_metric_name(metric_name_len: int, metric_id: int, experiment_name: str) -> str:
    metric_name = f"{METRIC_PREFIX}{experiment_name}_{metric_id}_"
    diff = metric_name_len - len(metric_name)
    metric = f"{metric_name}{''.zfill(diff)}"
    return metric


with Run(
    api_token='eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vbXItMTM3MzkuZGV2Lm5lcHR1bmUuYWkiLCJhcGlfdXJsIjoiaHR0cHM6Ly9tci0xMzczOS5kZXYubmVwdHVuZS5haSIsImFwaV9rZXkiOiI4OWNlNjE2ZS1iN2YyLTQ5NTctODI3YS1lNWZkNDY0MmJhZmUifQ==',
    project='team/stoi',
    run_id="".join(random.choice(string.ascii_letters) for _ in range(10)),
    family='my-custom-run-1',
    as_experiment=experiment_name) as run:
    step = 0
    metric_names = [generate_metric_name(metric_name_len, i, experiment_name) for i in
                    range(number_of_metrics_per_epoch)]
    while True:
        step += 1
        run.log(
            step=step,
            metrics={metric_names[j]: round(random.uniform(j, j + 1), 2) for j in range(number_of_metrics_per_epoch)}
        )

        print(f"{datetime.datetime.now()} Logged step {step}")
        time.sleep(20)
