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
pip install neptune-client-scale
```

## Example usage

```python
from neptune_scale import Run

run = Run(
    family="RunFamilyName",
    run_id="SomeUniqueRunIdentifier",
)

run.log(
    metrics={"Metric1": metric1_value, "Metric2": metric2_value},
    fields={"Field1": field1_value}
)

run.close()
```

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
| `family`         | `str`            | -       | Identifies related runs. All runs of the same lineage must have the same `family` value. That is, forking is only possible within the same family. Max length: 128 characters. |
| `run_id`         | `str`            | -       | Identifier of the run. Must be unique within the project. Max length: 128 characters. |
| `project`        | `str`, optional  | `None`  | Name of a project in the form `workspace-name/project-name`. If `None`, the value of the `NEPTUNE_PROJECT` environment variable is used. |
| `api_token`      | `str`, optional  | `None`  | Your Neptune API token or a service account's API token. If `None`, the value of the `NEPTUNE_API_TOKEN` environment variable is used. To keep your token secure, don't place it in source code. Instead, save it as an environment variable. |
| `resume`         | `bool`, optional | `False` | If `False` (default), creates a new run. To continue an existing run, set to `True` and pass the ID of an existing run to the `run_id` argument. To fork a run, use `from_run_id` and `from_step` instead. |
| `mode`           | `Literal`, `"async"` or `"disabled"` | `"async"` | Mode of operation. If set to `"disabled"`, the run doesn't log any metadata. |
| `as_experiment`  | `str`, optional  | `None` | Name of the experiment to associate the run with. Learn more about [experiments](https://docs-beta.neptune.ai/concepts) in the Neptune documentation. |
| `creation_time`  | `datetime`, optional | `None` | Custom creation time of the run. |
| `from_run_id`    | `str`, optional  | `None` | If forking off an existing run, ID of the run to fork from. |
| `from_step`      | `int`, optional  | `None` | If forking off an existing run, step number to fork from. |
| `max_queue_size` | `int`, optional  | 1M | Maximum number of operations queued for processing. 1 000 000 by default. You should raise this value if you see the `on_queue_full_callback` function being called. |
| `on_queue_full_callback` | `Callable[[BaseException, Optional[float]], None]`, optional | `None` | Callback function triggered when the queue is full. The function should take two arguments: (1) Exception that made the queue full. (2) Optional timestamp: When the exception was last raised. |
| `on_network_error_callback` | `Callable[[BaseException, Optional[float]], None]`, optional | `None` | Callback function triggered when a network error occurs. |
| `on_error_callback` | `Callable[[BaseException, Optional[float]], None]`, optional | `None` | The default callback function triggered when an unrecoverable error occurs. Applies if an error wasn't caught by other callbacks. In this callback you can choose to perform your cleanup operations and close the training script. |
| `on_warning_callback` | `Callable[[BaseException, Optional[float]], None]`, optional | `None` | Callback function triggered when a warning occurs. |

__Examples__

Create a new run:

```python
from neptune_scale import Run

with Run(
    project="team-alpha/project-x",
    api_token="h0dHBzOi8aHR0cHM6...Y2MifQ==",
    family="aquarium",
    run_id="likable-barracuda",
) as run:
    ...
```

> [!TIP]
> Find your API token in your user menu, in the bottom-left corner of the Neptune app.
>
> Or, to use shared API tokens for multiple users or non-human accounts, create a service account in your workspace settings.

Create a forked run and mark it as an experiment:

```python
with Run(
    family="aquarium",
    run_id="adventurous-barracuda",
    as_experiment="swim-further",
    from_run_id="likable-barracuda",
    from_step=102,
) as run:
    ...
```

Continue a run:

```python
with Run(
    family="aquarium",
    run_id="likable-barracuda",  # a Neptune run with this ID already exists
    resume=True,
) as run:
    ...
```

## `close()`

Waits for all locally queued data to be processed by Neptune (see [`wait_for_processing()`](#wait_for_processing)) and closes the run.

This is a blocking operation. Call the function at the end of your script, after your model training is completed.

__Examples__

```python
from neptune_scale import Run

run = Run(...)
run.log(...)

run.close()
```

If using a context manager, Neptune automatically closes the run upon exiting the context:

```python
with Run(...) as run:
    ...

# run is closed at the end of the context
```

## `log()`

Logs the specified metadata to a Neptune run.

You can log metrics, tags, and configurations. Pass the metadata as a dictionary `{key: value}` with

- `key`: path to where the metadata should be stored in the run.
- `value`: the piece of metadata to log.

For example, `{"parameters/learning_rate": 0.001}`. In the field path, each forward slash `/` nests the field under a namespace. Use namespaces to structure the metadata into meaningful categories.

__Parameters__

| Name          | Type                                               | Default | Description                                                               |
|---------------|----------------------------------------------------|---------|---------------------------------------------------------------------------|
| `step`        | `Union[float, int]`, optional                      | `None`  | Index of the log entry. Must be increasing. If not specified, the `log()` call increments the step starting from the highest already logged value. **Tip:** Using float rather than int values can be useful, for example, when logging substeps in a batch. |
| `timestamp`   | `datetime`, optional                               | `None`  | Time of logging the metadata. |
| `fields`      | `Dict[str, Union[float, bool, int, str, datetime, list, set]]`, optional  | `None` | Dictionary of configs or other values to log. Independent of the step value. Available types: float, integer, Boolean, string, and datetime. To log multiple values at once, pass multiple dictionaries. |
| `metrics`     | `Dict[str, float]`, optional                       | `None`  | Dictionary of metrics to log. Each metric value is associated with a step. To log multiple metrics at once, pass multiple dictionaries. |
| `add_tags`    | `Dict[str, Union[List[str], Set[str]]]`, optional  | `None`  | Dictionary of tags to add to the run, as a list of strings. Independent of the step value. |
| `remove_tags` | `Dict[str, Union[List[str], Set[str]]]`, optional  | `None`  | Dictionary of tags to remove from the run, as a list of strings. Independent of the step value. |

__Examples__

Create a run and log some metadata:

```python
from neptune_scale import Run

with Run(...) as run:
    run.log(
        fields={"parameters/learning_rate": 0.001},
        add_tags={"sys/tags": ["tag1", "tag2"]},
        metrics={"loss": 0.14, "acc": 0.78},
    )
```

Remove a tag:

```python
with Run(...) as run:
    run.log(remove_tags={"sys/tags": "tag2"})
```

You can pass the step when logging metrics:

```python
run.log(step=5, metrics={"loss": 0.09, "acc": 0.82})  # works if the previous step is no higher than 4
run.log(metrics={"loss": 0.08, "acc": 0.86})          # step index is set to "6"
...
```

**Note:** Calling `log()` without specifying the step still increments the index. To correlate logged values, make sure to send all metadata related to a step in a single `log()` call, or specify the step explicitly.

## `wait_for_submission()`

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
    run.log(...)
    ...
    run.wait_for_submission()
    run.log(fields={"scores/some_score": some_score_value})  # called once queued Neptune operations have been submitted
```

## `wait_for_processing()`

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
    run.log(...)
    ...
    run.wait_for_processing()
    run.log(fields={"scores/some_score": some_score_value})  # called once submitted data has been processed
```

## Getting help

For questions or comments, contact support@neptune.ai.
