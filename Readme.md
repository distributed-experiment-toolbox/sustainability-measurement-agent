# Sustainability Measurements Agent (SMA)

[![PyPI - Version](https://img.shields.io/pypi/v/sustainability-measurement-agent)](https://pypi.org/project/sustainability-measurement-agent/) | [![Upload Python Package](https://github.com/ISE-TU-Berlin/sustainability-measurement-agent/actions/workflows/python-publish.yml/badge.svg)](https://github.com/ISE-TU-Berlin/sustainability-measurement-agent/actions/workflows/python-publish.yml)

SMA is an open-source tool designed to help deploy, collect and report sustainability measurements on cloud-native applications. It is not itself a measurement tool, but rather a framework to orchestrate, combine and aggregate results from emerging suite of sustainability measurement tools. 


## Capabilities (planned)
 - Configuration driven operation
<!-- - Support for multiple measurement tools
   - Kepler
   - Scraphandre
   - cAdvisor
   - KubeWatt
   - Cloud Provider APIs (AWS, GCP, Azure)
   - KubeMetrics
-->
 - Support for multiple scenarios
   - Continuous monitoring and reporting (as a Service)
   - Experiment Measurements / Benchmarking
   - Ad-hoc measurements
   - Programmatic measurements via API
 - Multiple report formats
   - CSV
 - Post processing and aggregation
   - Pandas
   - Grafana Dashboards
 - CLI tool

## Use / Install
Make sure you have Python 3.8+ as well as hdf5 and jupyter installed. You may need to install additional dependencies depending on your setup, e.g., c-blosc lzo bzip2 gcc.
```bash
pip install sustainability-measurement-agent

touch main.py
```

```python
import sys
from sma import SustainabilityMeasurementAgent, Config, SMAObserver

config = Config.from_file("examples/minimal.yaml")
sma = SustainabilityMeasurementAgent(config)
sma.connect()
def wait_for_user() -> None:
    input("Hit Enter to to Stop Measuring...")

sma.run(wait_for_user)
sma.teardown()
report_location = config.report.get("location")
if report_location:
    print(f"Report written to {report_location}")
```

```bash
$ python main.py
$ Hit Enter to to Stop Measuring...
``` 

SMA also ships with a cli tool, that allows some basic operations:
```bash
$ sma --help
Usage: sma [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  fetch  Attempts to fetches measurements again using the provided
         configuration file.
  list   Lists all reports for a given configuration file.
  run    Run the Sustainability Measurement Agent with a given...
```


### Architecture and Lifecycle

SMA follows a simple lifecycle:
[![SMA Lifecycle](docs/lifecycle.svg)](docs/lifecycle.svg)

Fully driven by configuration files, SMA connects to external services (e.g., Prometheus), deploys measurement tools (e.g., Kepler), and once instrcuted via the API (`run()`), starts the observation according to the configured mode (trigger, timer, continuous). Finally, SMA collects the measurement data, generates reports, and tears down any deployed infrastructure (if configured to do so).

SMA exposes an observer API (`SMAObserver`), which allows to hook into the lifecycle events (see the diagram) to extend SMA's functionality programmatically, or to integrate into experiment frameworks.
Observers are notified in registration order: those passed to the constructor first, then config-loaded modules. Only an exception in `onRunStart` aborts a run; every other hook's exceptions are logged. Right before `onRunStart`, `onRunParameters(**kwargs)` hands observers the keyword arguments of `run()`, exactly as the trigger receives them. An observer that needs per-run input before the treatment keeps a copy (the telek6 module uses it to prepare k6 ahead of the treatment, see `modules/telek6/main.py`).

*Modules* can optionally be used to add functionality, such as installing monitoring infrascture, preparing deployments, running workloads, and enhancing reports.


### Configuration

SMA is configured via YAML files. See the `examples/` folder for sample configurations. The configuration allows to specify which measurement tools to use, how to deploy them, and how to collect and report the measurements.

#### Configuration Structure

The configuration file consists of the following main sections:

##### 1. Version
```yaml
sma:
  version: 0.2.0
```
Specifies the SMA configuration schema version.

##### 3. Observability Services
```yaml
services:
  prometheus:
    address: http:/<IP>
```

Defines external services used for data collection, sofar we only support Prometheus.

| Field | Description |
|-------|-------------|
| `prometheus.address` | URL of the Prometheus server for metrics collection |

##### 5. Observation
```yaml
observation:
  mode: trigger 
  window:
    left: 10s   
    right: 10s 
  max_duration: 10s
  targets:
    - name: sut
      namespace: kubeai
```

| Field                                                                        | Options/Type | Description                                                                                                                           |
|------------------------------------------------------------------------------|--------------|---------------------------------------------------------------------------------------------------------------------------------------|
| `mode`                                                                       | `trigger`, `timer`, `module` | **trigger**: Measurement starts/stops based on a Python function that blocks<br>**timer**: Measurement based on time window settings\ 
 <br>**module**: run a trigger function from a module, e.g. a workload runner |
| `trigger_module`                                                             | When in *module* mode, specify the module name to call the trigger from |
| `window.left`                                                                | Duration (e.g., `10s`) | Time to capture before the measurement trigger                                                                                        |
| `window.right`                                                               | Duration (e.g., `10s`) | Time to capture after the measurement trigger                                                                                         |
| `max_duration`                                                               | Duration (e.g., `10s`) | Optional total measurement duration, before a run is interuppted.                                                                     |
| `targets[].name`                                                             | String | Name identifier for the target workload                                                                                               |
| `targets[].namespace`                                                        | String | Kubernetes namespace of the target workload                                                                                           |

Both `left` and `right` windows are optional and mean, SMA will wait before handing off the measurement to the next API call.
The `max_duration` is optional and specifies the maximum time to run the measurement, before it is interrupted. For the `time` trigger mode, the treatment will be stopped after the `max_duration` has elapsed (so expect to see a warning in the logs if the treatment is interrupted).

##### 6. Measurements
```yaml
measurements:
  # Custom PromQL query
  - measurement_name:
      type: aggregate
      layer: pod         
      query: <PromQL query>
      step: 30           
      unit: watts       
      target:
        - all        
```  
<!--
  # Prepared query (predefined)
  - pod-consumption:
      type: aggregate
      layer: pod
      prepared_query: pod_kepler_energy_consumption
      step: 30
      unit: watts
      sensors:
        - kepler
      target:
        - sut
-->

| Field | Options/Type                          | Description |
|-------|---------------------------------------|-------------|
| `type` | `raw`, `aggregate`, `counter`, `guage` | **raw**: Unprocessed metric data<br>**aggregate**: Aggregated/computed metrics |
| `layer` | `substrate`,`pod`, `node`, `process`  | The abstraction level being measured |
| `query` | PromQL string                         | Prometheus query to retrieve the metric. Supports template variables:<br>`${namespace}`: Replaced with target namespace<br>`${SMA_SELECTORS}`: Additional label filters |
| `sensors` | List of strings                       | List of sensors required for this measurement (used with `prepared_query`) |
| `step` | Integer (seconds)                     | Sampling interval in seconds |
| `unit` | String                                | Measurement unit (e.g., `watts`, `joules`, `cpu_time_seconds`, `metadata`) |
| `target` | List of strings                       | Target names to measure, or `all` for all defined targets |

<!--| `prepared_query` | String                                | Use a predefined query from SMA for common metrics (alternative to `query`) |-->

**Query Methods:**
- **Custom Query**: Use `query` field with custom PromQL
- **Prepared Query**: Use `prepared_query` field with predefined SMA queries (e.g., `pod_kepler_energy_consumption`, `node_scaphandre_energy_consumption`)

**Example Measurements:**
- **CPU Usage**: Container CPU usage across namespaces
- **Wall Power**: Physical power consumption from smart plugs (e.g., Shelly)
- **Pod Power (Kepler)**: Per-pod energy consumption via Kepler
- **Node Power (Scaphandre)**: Host-level power measurements
- **Container Info**: Metadata about running containers

##### 7. Report
```yaml
report:
  format: csv          # Output format
  location: reports/${startTime}_${runHash}/
  filename: "${name}_${startTime}.csv"
```
<!--
  post_processor:
    - name: overview
      type: notebook
      notebook: reports/overview.ipynb
    - name: validation
      type: sma
      module: sma.validation.Validator
-->

| Field | Type | Description |
|-------|------|-------------|
| `format` | String | Output format (currently supports `csv`) |
| `location` | String | Directory path for reports. Supports template variables:<br>`${startTime}`: Timestamp when measurement started<br>`${runHash}`: Unique hash for the measurement run |
| `filename` | String | File naming pattern. Supports template variables:<br>`${name}`: Measurement name<br>`${startTime}`: Measurement start time |
<!--
| `post_processor` | List | (Optional) Post-processing steps to run after data collection |
| `post_processor[].name` | String | Identifier for the post-processor |
| `post_processor[].type` | `notebook`, `sma` | Type of post-processor: `notebook` for Jupyter notebooks, `sma` for built-in SMA modules |
| `post_processor[].notebook` | String | Path to Jupyter notebook (when `type: notebook`) |
| `post_processor[].module` | String | Python module path (when `type: sma`) |
-->

Modules can add their own files to the report directory through the `onReport` event.

### API 

<!-- TODO: Add API documentation -->

## Development

1. Install pipenv: `pip install pipenv`
2. Install dependencies: `pipenv install --dev`
3. Activate virtual environment: `pipenv shell`
4. Do your thing. Eventually with tests.
5. If you add new dependencies, run `pipenv lock --pre` to update the `Pipfile.lock`.
6. If you want to release a new version, update the version in `pyproject.toml` and run `pipenv run pyproject-pipenv --fix` and commit the changes.
7. Tag your release: `git tag vx.y.z` and `git push --tags`
8. Create a new release on GitHub, this will trigger the upload to PyPI via GitHub Actions.

### Acknowledgements

This project builds upon prior work done in the [OXN project](https://github.com/nymphbox/oxn), [GOXN project](https://github.com/JulianLegler/goxn) and [CLUE](https://github.com/ISE-TU-Berlin/Clue) and is part of the research at [ISE-TU Berlin](https.//www.tu.berlin/ise).