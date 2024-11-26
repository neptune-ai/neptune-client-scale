# Neptune Scale client

> [!NOTE]
> This package only works with the `3.0` version of neptune.ai called Neptune Scale, which is in beta.
>
> You can't use the Scale client with the stable Neptune `2.x` versions currently available to SaaS and self-hosting customers. For the Python client corresponding to Neptune `2.x`, see https://github.com/neptune-ai/neptune-client.

**What is Neptune?**

Neptune is an experiment tracker. It enables researchers to monitor their model training, visualize and compare model metadata, and collaborate on AI/ML projects within a team.

**What's different about Neptune Scale?**

Neptune Scale is the next major version of Neptune. It's built on an entirely new architecture for ingesting and rendering data, with a focus on responsiveness and accuracy at scale.

Neptune Scale supports forked experiments, with built-in mechanics for retaining run ancestry. This way, you can focus on analyzing the latest runs, but also visualize the full history of your experiments.

## Installation

```bash
pip install neptune-scale
```

### Configure API token and project

1. Log in to your Neptune Scale workspace.
1. Get your API token from your user menu in the bottom left corner.

    > If you're a workspace admin, you can also set up a service account. This way, multiple people or machines can share the same API token. To get started, access the workspace settings via the user menu.

1. In the environment where neptune-scale is installed, save your API token to the `NEPTUNE_API_TOKEN` environment variable:

    ```
    export NEPTUNE_API_TOKEN="h0dHBzOi8aHR0cHM6...Y2MifQ=="
    ```

1. Create a project, or find an existing project you want to send the run metadata to.

    To create a project via API:

    ```python
    from neptune_scale import create_project

    create_project(
        name="project-x",
        workspace="team-alpha",
    )
    ```

1. (optional) In the environment where neptune-scale is installed, save your full project path to the `NEPTUNE_PROJECT` environment variable:

    ```bash
    export NEPTUNE_PROJECT="team-alpha/project-x"
    ```

    > If you skip this step, you need to pass the project name as an argument each time you start a run.

You're ready to start using Neptune Scale.

For more help with setup, see [Get started][scale-docs] in the Neptune documentation.

## Example usage

Create an experiment:

```python
from neptune_scale import Run

run = Run(
    experiment_name="ExperimentName",
    run_id="SomeUniqueRunIdentifier",
)
```

Then, call logging methods on the run and pass the metadata as a dictionary.

Log configuration or other simple values with [`log_configs()`](#log_configs):

```python
run.log_configs(
    {
        "learning_rate": 0.001,
        "batch_size": 64,
    }
)
```

Inside a training loop or other iteration, use [`log_metrics()`](#log_metrics) to append metric values:

```python
# inside a loop
for step in range(100):
    run.log_metrics(
        data={"acc": 0.89, "loss": 0.17},
        step=step,
    )
```

To help identify and group runs, you can apply tags:

```python
run.add_tags(tags=["tag1", "tag2"])
```

The run is stopped when exiting the context or the script finishes execution, but you can use [`close()`](#close) to stop it once logging is no longer needed:

```python
run.close()
```

To explore your experiment, open the project in Neptune and navigate to **Runs**. For an example, [see the demo project &rarr;][demo-project]

For more instructions, see the Neptune documentation:

- [Quickstart][quickstart]
- [Create an experiment][new-experiment]
- [Log metadata][log-metadata]

## API reference

### `Run`

Representation of experiment tracking metadata logged with Neptune Scale.

#### Initialization

Initialize with the class constructor:

```python
from neptune_scale import Run

run = Run(...)
```

or using a context manager:

```python
from neptune_scale import Run

with Run(...) as run:
    ...
```

__Parameters__

| Name             | Type             | Default | Description                                                               |
|------------------|------------------|---------|---------------------------------------------------------------------------|
| `run_id`         | `str`            | -       | Identifier of the run. Must be unique within the project. Max length: 128 bytes. |
| `project`        | `str`, optional  | `None`  | Name of a project in the form `workspace-name/project-name`. If `None`, the value of the `NEPTUNE_PROJECT` environment variable is used. |
| `api_token`      | `str`, optional  | `None`  | Your Neptune API token or a service account's API token. If `None`, the value of the `NEPTUNE_API_TOKEN` environment variable is used. To keep your token secure, don't place it in source code. Instead, save it as an environment variable. |
| `resume`         | `bool`, optional | `False` | If `False` (default), creates a new run. To continue an existing run, set to `True` and pass the ID of an existing run to the `run_id` argument. To fork a run, use `fork_run_id` and `fork_step` instead. |
| `mode`           | `"async"` or `"disabled"` | `"async"` | Mode of operation. If set to `"disabled"`, the run doesn't log any metadata. |
| `experiment_name`  | `str`, optional  | `None` | Name of the experiment to associate the run with. Learn more about [experiments][experiments] in the Neptune documentation. |
| `creation_time`  | `datetime`, optional | `datetime.now()` | Custom creation time of the run. |
| `fork_run_id`    | `str`, optional  | `None` | The ID of the run to fork from. |
| `fork_step`      | `int`, optional  | `None` | The step number to fork from. |
| `max_queue_size` | `int`, optional  | 1M | Maximum number of operations queued for processing. 1 000 000 by default. You should raise this value if you see the `on_queue_full_callback` function being called. |
| `on_queue_full_callback` | `Callable[[BaseException, Optional[float]], None]`, optional | `None` | Callback function triggered when the queue is full. The function must take as an argument the exception that made the queue full and, as an optional argument, a timestamp of when the exception was last raised. |
| `on_network_error_callback` | `Callable[[BaseException, Optional[float]], None]`, optional | `None` | Callback function triggered when a network error occurs. |
| `on_error_callback` | `Callable[[BaseException, Optional[float]], None]`, optional | `None` | The default callback function triggered when an unrecoverable error occurs. Applies if an error wasn't caught by other callbacks. In this callback you can choose to perform your cleanup operations and close the training script. For how to end the run in this case, use [`terminate()`](#terminate). |
| `on_warning_callback` | `Callable[[BaseException, Optional[float]], None]`, optional | `None` | Callback function triggered when a warning occurs. |

__Examples__

Create a new run:

```python
from neptune_scale import Run

with Run(
    project="team-alpha/project-x",
    api_token="h0dHBzOi8aHR0cHM6...Y2MifQ==",
    run_id="likable-barracuda",
) as run:
    ...
```

For help, see [Create an experiment][new-experiment] in the Neptune docs.

> [!TIP]
> Find your API token in your user menu, in the bottom-left corner of the Neptune app.
>
> Or, to use shared API tokens for multiple users or non-human accounts, create a service account in your workspace settings.

To restart an experiment, create a forked run:

```python
with Run(
    run_id="adventurous-barracuda",
    experiment_name="swim-further",
    fork_run_id="likable-barracuda",
    fork_step=102,
) as run:
    ...
```

Continue a run:

```python
with Run(
    run_id="likable-barracuda",  # a Neptune run with this ID already exists
    resume=True,
) as run:
    ...
```

### `close()`

The regular way to end a run. Waits for all locally queued data to be processed by Neptune (see [`wait_for_processing()`](#wait_for_processing)) and closes the run.

This is a blocking operation. Call the function at the end of your script, after your model training is completed.

__Examples__

```python
from neptune_scale import Run

run = Run(...)

# logging and training code

run.close()
```

If using a context manager, Neptune automatically closes the run upon exiting the context:

```python
with Run(...) as run:
    ...

# run is closed at the end of the context
```

### `log_configs()`

Logs the specified metadata to a Neptune run.

You can log configurations or other single values. Pass the metadata as a dictionary `{key: value}` with

- `key`: path to where the metadata should be stored in the run.
- `value`: the piece of metadata to log.

For example, `{"parameters/learning_rate": 0.001}`. In the attribute path, each forward slash `/` nests the attribute under a namespace. Use namespaces to structure the metadata into meaningful categories.

Any `datetime` values that don't have the `tzinfo` attribute set are assumed to be in the local timezone.

__Parameters__

| Name          | Type                                               | Default | Description                                                               |
|---------------|----------------------------------------------------|---------|---------------------------------------------------------------------------|
| `data`      | `Dict[str, Union[float, bool, int, str, datetime, list, set, tuple]]`, optional  | `None` | Dictionary of configs or other values to log. Available types: float, integer, Boolean, string, and datetime. |

__Examples__

Create a run and log metadata:

```python
from neptune_scale import Run

with Run(...) as run:
    run.log_configs(
        data={
            "parameters/learning_rate": 0.001,
            "parameters/batch_size": 64,
        },
    )
```

### `log_metrics()`

Logs the specified metrics to a Neptune run.

You can log metrics representing a series of numeric values. Pass the metadata as a dictionary `{key: value}` with

- `key`: path to where the metadata should be stored in the run.
- `value`: the piece of metadata to log.

For example, `{"metrics/accuracy": 0.89}`. In the attribute path, each forward slash `/` nests the attribute under a namespace. Use namespaces to structure the metadata into meaningful categories.

__Parameters__

| Name        | Type                                     | Default | Description                                                                                                                                                                                                                                                          |
|-------------|------------------------------------------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `data`      | `Dict[str, Union[float, int]]` | `None`  | Dictionary of metrics to log. Each metric value is associated with a step. To log multiple metrics at once, pass multiple key-value pairs.                                                                                                                           |
| `step`      | `Union[float, int]`           | `None`  | Index of the log entry. Must be increasing. <br> **Tip:** Using float rather than int values can be useful, for example, when logging substeps in a batch. |
| `timestamp` | `datetime`, optional                     | `None`  | Time of logging the metadata. If not provided, the current time is used. If provided, and `timestamp.tzinfo` is not set, the time is assumed to be in the local timezone.                                                                                            |

__Examples__

Create a run and log metrics:

```python
from neptune_scale import Run

with Run(...) as run:
    run.log_metrics(
        data={"loss": 0.14, "acc": 0.78},
        step=1.2,
    )
```

**Note:** To correlate logged values, make sure to send all metadata related to a step in a single `log_metrics()` call, or specify the step explicitly.

When the run is forked off an existing one, the step can't be smaller than the step value of the fork point.

### `add_tags()`

Adds the list of tags to the run.

__Parameters__

| Name          | Type                                         | Default | Description                                                               |
|---------------|----------------------------------------------------|---------|---------------------------------------------------------------------------|
| `tags`        | `Union[List[str], Set[str], Tuple[str]]`                 | - | List or set of tags to add to the run. |
| `group_tags`  | `bool`, optional                             | `False`  | Add group tags instead of regular tags. |

__Example__

```python
with Run(...) as run:
    run.add_tags(tags=["tag1", "tag2", "tag3"])
```

### `remove_tags()`

Removes the specified tags from the run.

__Parameters__

| Name          | Type                                         | Default | Description                                                               |
|---------------|----------------------------------------------------|---------|---------------------------------------------------------------------------|
| `tags`        | `Union[List[str], Set[str], Tuple[str]]`                 | - | List or set of tags to remove from the run. |
| `group_tags`  | `bool`, optional                             | `False`  | Remove group tags instead of regular tags. |

__Example__

```python
with Run(...) as run:
    run.remove_tags(tags=["tag2", "tag3"])
```

### `wait_for_submission()`

Waits until all metadata is submitted to Neptune for processing.

When submitted, the data is not yet saved in Neptune (see [`wait_for_processing()`](#wait_for_processing)).

__Parameters__

| Name      | Type              | Default | Description                                                               |
|-----------|-------------------|---------|---------------------------------------------------------------------------|
| `timeout` | `float`, optional | `None`  | In seconds, the maximum time to wait for submission.                      |
| `verbose` | `bool`, optional  | `True`  | If True (default), prints messages about the waiting process.             |

__Example__

```python
from neptune_scale import Run

with Run(...) as run:
    run.log_configs(...)
    ...
    run.wait_for_submission()
    run.log_metrics(...)  # called once queued Neptune operations have been submitted
```

### `wait_for_processing()`

Waits until all metadata is processed by Neptune.

Once the call is complete, the data is saved in Neptune.

__Parameters__

| Name      | Type              | Default | Description                                                               |
|-----------|-------------------|---------|---------------------------------------------------------------------------|
| `timeout` | `float`, optional | `None`  | In seconds, the maximum time to wait for processing.                      |
| `verbose` | `bool`, optional  | `True`  | If True (default), prints messages about the waiting process.             |

__Example__

```python
from neptune_scale import Run

with Run(...) as run:
    run.log_configs(...)
    ...
    run.wait_for_processing()
    run.log_metrics(...)  # called once submitted data has been processed
```

### `terminate()`

In case an unrecoverable error is encountered, you can terminate the failed run in your error callback.

**Note:** This effectively disables processing in-flight operations as well as logging new data. However,
the training process isn't interrupted.

__Example__

```python
from neptune_scale import Run

def my_error_callback(exc):
    run.terminate()


run = Run(..., on_error_callback=my_error_callback)
```

---

## Getting help

For help, contact support@neptune.ai.


[scale-docs]: https://docs-beta.neptune.ai/setup
[experiments]: https://docs-beta.neptune.ai/experiments
[log-metadata]: https://docs-beta.neptune.ai/log_metadata
[new-experiment]: https://docs-beta.neptune.ai/new_experiment
[quickstart]: https://docs-beta.neptune.ai/quickstart
[demo-project]: https://scale.neptune.ai/o/neptune/org/LLM-training-example/runs/compare?viewId=9d0e03d5-d0e9-4c0a-a546-f065181de1d2&dash=charts&compare=uItSQytpSbTH0c84P6iKGycQhv1rZr-qt4Z-CzEVBwD0
