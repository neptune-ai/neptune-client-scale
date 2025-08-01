<div align="center">
    <img src="https://raw.githubusercontent.com/neptune-ai/neptune-client/assets/readme/Github-cover-022025.png" width="1500" />
 <h1>neptune.ai</h1>
</div>

<div align="center">
  <a href="https://docs.neptune.ai/quickstart/">Quickstart</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://neptune.ai/">Website</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://docs.neptune.ai/">Docs</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/neptune-ai/scale-examples">Examples</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://neptune.ai/blog">Blog</a>
&nbsp;
  <hr />
</div>

Neptune is an experiment tracker purpose-built for foundation model training.

With Neptune, you can monitor thousands of per-layer metrics—losses, gradients, and activations—at any scale. Visualize them with no lag and no missed spikes. Drill down into logs and debug training issues fast. Keep your model training stable while reducing wasted GPU cycles.

<a href="https://youtu.be/0J4dsEq8i08"><b>Watch a 3min explainer video →</b></a>
&nbsp;

<a href="https://scale.neptune.ai/o/examples/org/LLM-Pretraining/reports/9e6a2cad-77e7-42df-9d64-28f07d37e908"><b>Play with a live example project in the Neptune app  →</b></a>
&nbsp;

## Get started

Neptune consists of a Python API and a web application.

Install the Python client library:

```bash
pip install neptune-scale
```

Configure the API token and project:

1. Log in to your Neptune workspace.
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

You're ready to start using Neptune.

For more help with setup, see [Get started][docs] in the Neptune documentation.

## Example usage

Create an experiment:

```python
from neptune_scale import Run

run = Run(experiment_name="MyExperimentName")
```

Then, call logging methods on the run and pass the metadata as a dictionary.

Log configuration or other simple values with [`log_configs()`][log_configs]:

```python
run.log_configs(
    {
        "learning_rate": 0.001,
        "batch_size": 64,
    }
)
```

Inside a training loop or other iteration, use [`log_metrics()`][log_metrics] to append metric values:

```python
# inside a loop
for step in range(100):
    run.log_metrics(
        data={"acc": 0.89, "loss": 0.17},
        step=step,
    )
```

[Upload files][log-files]:

```python
run.assign_files(
    {
        "dataset/data_sample": "sample_data.csv",
        "dataset/image_sample": "input/images/img1.png",
    }
)
```

To help organize and group runs, apply tags:

```python
run.add_tags(tags=["testing", "data v1.0"])
```

The run is stopped when exiting the context or the script finishes execution, but you can use [`close()`][close] to stop it once logging is no longer needed:

```python
run.close()
```

To explore your experiment, open the project in Neptune and navigate to **Runs**. For an example, [see the demo project &rarr;][demo-project]

For more instructions, see the Neptune documentation:

- [Quickstart][quickstart]
- [Create an experiment][new-experiment]
- [Log metadata][log-metadata]
- [API reference][api-ref]

## Getting help

For help and support, visit our [Support Center](https://support.neptune.ai/).

&nbsp;
<hr />

## People behind Neptune

Created with :heart: by the [neptune.ai team &rarr;](https://neptune.ai/jobs#team)


[api-ref]: https://docs.neptune.ai/run
[close]: https://docs.neptune.ai/run/close
[docs]: https://docs.neptune.ai/setup
[experiments]: https://docs.neptune.ai/experiments
[log-files]: https://docs.neptune.ai/log_files
[log-metadata]: https://docs.neptune.ai/log_metadata
[log_configs]: https://docs.neptune.ai/run/log_configs
[log_metrics]: https://docs.neptune.ai/run/log_metrics
[new-experiment]: https://docs.neptune.ai/create_experiment
[quickstart]: https://docs.neptune.ai/quickstart
[demo-project]: https://scale.neptune.ai/o/examples/org/LLM-Pretraining/reports/9e6a2cad-77e7-42df-9d64-28f07d37e908
