# Running setup

These set of scripts are supposed to be run from your local machine (personal workstation/laptop).
Since many of us use Macbooks/ ARM laptops, binaries built locally can't be run on the deployed VMs.
Furthermore, we can't expect to keep personal devices open overnight while experiments finish.

Hence, the scripts will use one of the client VMs (pre-deployed / deployed by the scripts themselves),
to build binaries and run as coordinator to collect logs as experiments finish.

# System requirements

The local machine can be anything. However, it is not tested for Windows.
Works for Unix based systems. Windows may have issues with path names (`/` vs `\`).
Required system packages: `OpenSSH`, `rsync`, `tmux`.
These must be available in BOTH locally and in VMs.
VMs must be running SSH on port 22.
If you use the scripts to deploy, all of these will be taken care of by default.


# Experiment flow

Each experiment has to defined entirely in one toml config file (see `sample_config.toml`).
The experiment flow goes as follows:

Deploy VMs ➡️ Deploy experiments (ie build binaries and generate configs and distribute them in VMs) ➡️
Run experiments ➡️ Copy logs back to local machine ➡️ Plot results ➡️ Teardown VMs.

All of these can be done in a single invocation of the script.
However, it is rather time consuming and the deployment process can be error-prone
(This is inevitable; no amount of error handling/retries can automate this).
Hence we also provide subcommands for each step.

# Script environment setup

The scripts expect a Python virtualenv.
Run the following:

```bash
virtualenv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

For each new shell thereafter, run `source .venv/bin/activate` before running any other command.

# Running everything all at once

Run the following from the root of the repo.
```bash
python3 scripts -c path/to/experiment.toml
```

It will create a timestamped directory in the working directory specified in the config. (Henceforth we will call that `workdir`.)
Then it will deploy and prepare VMs using terraform (based on the scripts in `deployment/`),
and will choose a dev VM to use a coordinator.

Once all the configs are generated and binaries built (this is a long blocking operation),
for each run of each parameter sweep of each experiment, it will create a `arbiter-*.sh` script.
Running this script on the dev VM performs that run and then copies back the logs.

For the overall experiment plan, the script will then generate a `job_<timestamp>.sh` in `workdir/jobs`.
The entire `workdir` is rsynced to the dev VM where this job script is run.
At this point, the local VM will start showing a status bar showing experiment progress.
When this appears, it is safe to `Ctrl+C` the script.
The progress happens in dev VM anyway.
If you don't, the experiments run to completion, the logs are copied back to your local machine
and the results are plotted (in `workdir/results`).

Finally, teardown the setup using `python3 scripts teardown -c path/to/toml -d path/to/workdir`


# Running experiments one step at a time.

To deploy: `python3 scripts deploy -c path/to/toml`

To deploy experiments: `python3 scripts deploy-experiments -c path/to/toml -d path/to/workdir`

To run experiments: `python3 scripts run-experiments -c path/to/toml -d path/to/workdir`

To check running status of experiments: `python3 scripts status -c path/to/toml -d path/to/workdir`

To copy logs back to local VM: `python3 scripts sync-local -c path/to/toml -d path/to/workdir`

To plot results: `python3 scripts results -c path/to/toml -d path/to/workdir`

# Helper methods

To clean dev VM: `python3 scripts clean-dev -c path/to/toml -d path/to/workdir`

To SSH into a VM: `python3 scripts ssh -c path/to/toml -d path/to/workdir [-n name_of_vm]`

To run a command in all VMs: `python3 scripts run-command -c path/to/toml -d path/to/workdir -cmd "command"`

To reuse the same deployment in a new experiment setup: `python3 scripts reuse-deployment -c path/to/new/experiment/toml -d path/to/old/workdir`


# What if something goes wrong?

Parts of the experiment pipeline may fail for reasons beyond our control.
In that case, it is ok to delete the corresponding subdirectories, except `workdir/deployment`.

Deleting `workdir/deployment` makes Terraform lose all state and rerunning `deploy` will create new VMs.

It is ok (and advised) to delete `workdir/experiments` entirely if the `deploy-experiments` stage fails (eg, unable to build or configs are corrupted etc.)

If an experiment stops midway (say the space in the dev VM is exhausted), copy all the logs to local using `sync-local` and redo `run-experiments`.
(Meticulously check if the logs are corrupted or not, the script WILL NOT check for it.)
It will only run the experiments that have not run before.

If plotting fails, you can remove `workdir/results` entirely.

Most of the experiment is saved using pickle, so as to save progress and restart as fast as possible.


# Writing TOML configs

Please see the example tomls here.

Some points:

- `deployment_config.mode` can be `manual`, which requires you to specify `deployment_config.node_list`.
- `deployment_config.mode` can be some `val`, if so, `deployment/azure-tf/setups/val.tfvars` must exist.
- `deployment_config.layout[]` can be used to specify layouts for WAN experiments, the order of nodes per region is the same used in the tfvars file used.

- You can specify a global `node_config` and `client_config`, and then override specific fields in each experiment.
- Each experiment will also have `experiments.sweeping_parameters` section. If you sweep over multiple fields, the scripts will create a cross-product and make sub-experiments for each element in the cross-product set.
- The final config for each sub-experiment then must conform to the `Config` struct in `src/config`.

- The result plotter functions are defined in `results.py`. The params they expect can be passed through the scripts.


# Known issues

- Can't handle cases where VMs of multiple build triples. For eg, if the dev VM uses `x86_64-unkown-linux-gnu`, every other VM must also use it. Can't mix this with ARM machines.
- Currently only supports Terraform. Docker and Azure C-ACI deployments are underway (cc: @nacrooks).

Happy experimenting!