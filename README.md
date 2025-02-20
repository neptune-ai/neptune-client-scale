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
    from neptune_scale.projects import create_project

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

See [API reference][api-ref] in the Neptune documentation.

---

## Getting help

For help, contact support@neptune.ai.


[api-ref]: https://docs-beta.neptune.ai/run
[scale-docs]: https://docs-beta.neptune.ai/setup
[experiments]: https://docs-beta.neptune.ai/experiments
[log-metadata]: https://docs-beta.neptune.ai/log_metadata
[new-experiment]: https://docs-beta.neptune.ai/new_experiment
[quickstart]: https://docs-beta.neptune.ai/quickstart
[demo-project]: https://scale.neptune.ai/o/neptune/org/LLM-training-example/runs/compare?viewId=9d0e03d5-d0e9-4c0a-a546-f065181de1d2&dash=charts&compare=uItSQytpSbTH0c84P6iKGycQhv1rZr-qt4Z-CzEVBwD0
