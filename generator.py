import asyncio
import datetime
import os
import random
import string
import time
import datetime
import random
import string
import time

from neptune_scale import Run

METRIC_PREFIX = "test/"
METRIC_NUMBER_LEN = 10


def conf(key: str, default: str) -> str:
    value = os.getenv(key, default)
    print(f"key: {key} value: {value} default: {default}")
    return value


number_of_runs = int(conf("NEPTUNE_PERFORMANCE_TEST_NUMBER_OF_RUNS", "1"))

number_of_metrics_per_epoch = int(conf("NEPTUNE_PERFORMANCE_TEST_NUMBER_OF_METRICS_PER_EPOCH", "10000"))
time_between_epochs = float(conf("NEPTUNE_PERFORMANCE_TEST_TIME_BETWEEN_EPOCHS", "60"))  # in seconds

api_token = conf("NEPTUNE_API_TOKEN",
                 'eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vbXItMTM3MzkuZGV2Lm5lcHR1bmUuYWkiLCJhcGlfdXJsIjoiaHR0cHM6Ly9tci0xMzczOS5kZXYubmVwdHVuZS5haSIsImFwaV9rZXkiOiI4OWNlNjE2ZS1iN2YyLTQ5NTctODI3YS1lNWZkNDY0MmJhZmUifQ==')
project = conf("NEPTUNE_PROJECT", 'team/stoi')

metric_name_len = int(conf("NEPTUNE_METRIC_NAME_LEN", "120"))

fork_duration_max = int(conf("NEPTUNE_FORK_DURATION_MAX_SECONDS", str(120)))
fork_duration_min = int(conf("NEPTUNE_FORK_DURATION_MIN_SECONDS", str(60)))
experiment_name_prefix = conf("NEPTUNE_EXPERIMENT_NAME_PREFIX",
                              "".join(random.choice(string.ascii_letters) for _ in range(10)))


def generate_metric_name(metric_name_len: int, metric_id: int, experiment_name: str) -> str:
    metric_name = f"{METRIC_PREFIX}_{metric_id}_"
    diff = metric_name_len - len(metric_name)
    metric = f"{metric_name}{''.zfill(diff)}"
    return metric


def partition(lst, n):
    """
    https://stackoverflow.com/a/14861842
    """
    division = len(lst) / float(n)
    return [lst[int(round(division * i)): int(round(division * (i + 1)))] for i in range(n)]


stats_counter = 0


async def generate_experiment(experiment_name: str):
    """
    The experiment involves logging metrics for each epoch. After each fork, the function waits for a random duration
    between 1 and 20 seconds. Each experiment also has a startup delay of between 1 and 60 seconds.
    """
    family = "".join(random.choice(string.ascii_letters) for _ in range(10))
    fork_depth = 0
    current_run_id = "".join(random.choice(string.ascii_letters) for _ in range(10))
    current_run = Run(
        api_token=api_token,
        project=project,
        run_id=current_run_id,
        family=family,
        as_experiment=experiment_name)

    metric_names = [generate_metric_name(metric_name_len, i, experiment_name) for i in
                    range(number_of_metrics_per_epoch)]

    await asyncio.sleep(random.randint(1, 60))
    epoch = -1
    while True:
        fork_duration = random.randint(fork_duration_min, fork_duration_max)
        print(f"DEBUG:{datetime.datetime.now()} fork duration {fork_duration}")
        fork_start = time.time()
        while int(time.time() - fork_start) < fork_duration:
            start_time = time.time()
            epoch += 1
            chunked = partition(range(number_of_metrics_per_epoch), random.randint(1, 10))
            for chunk in chunked:
                metrics = {metric_names[j]: round(random.uniform(j, j + 1), 2) for j in chunk}
                current_run.log(
                    step=epoch,
                    metrics=metrics
                )
                global stats_counter
                stats_counter += len(metrics)
                # Setting the delay to 0 provides an optimized path to allow other tasks to run.
                await asyncio.sleep(0)

            epoch_time = (time.time() - start_time)
            if epoch_time < time_between_epochs:
                await asyncio.sleep(time_between_epochs - epoch_time)
            else:
                print("Houston, we have a problem")

        epoch = random.randint(int(epoch / 2), int(epoch))

        print(f"DEBUG:{datetime.datetime.now()} forking from {current_run_id} at epoch {epoch}")

        new_run_id = "".join(random.choice(string.ascii_letters) for _ in range(10))
        current_run = Run(
            api_token=api_token,
            project=project,
            run_id=new_run_id,
            family=family,
            as_experiment=experiment_name,
            from_run_id=current_run_id,
            from_step=epoch)
        current_run_id = new_run_id
        fork_depth += 1
        await asyncio.sleep(random.randint(1, 20))


async def print_stats():
    last_update = time.time()
    while True:
        await asyncio.sleep(10)
        elapsed_time = (time.time() - last_update)
        global stats_counter
        print(
            f"DEBUG:{datetime.datetime.now()} stats: {stats_counter} metrics in {int(elapsed_time)} seconds = {int(stats_counter / elapsed_time):,} dp/s")
        stats_counter = 0
        last_update = time.time()


async def run():
    runs = [generate_experiment(f"{experiment_name_prefix}_{i}") for i in range(number_of_runs)]
    runs.append(print_stats())
    await asyncio.gather(*runs)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
