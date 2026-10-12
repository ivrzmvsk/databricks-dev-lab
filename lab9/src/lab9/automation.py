"""Start once, monitor the exact run/update, and report terminal outcomes."""

import json
import signal
import threading
import time

from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.jobs import NotebookTask, SubmitTask

from .auth import target_settings
from .preflight import preflight
from .reporting import Report, emit, job_snapshot, pipeline_snapshot, state_name

JOB_TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
PIPELINE_TERMINAL = {"COMPLETED", "FAILED", "CANCELED"}


class Automation:
    def __init__(
        self,
        client,
        inventory,
        report_path,
        timeout=3600,
        interval=15,
        clock=time.monotonic,
        sleep=time.sleep,
        target="personal",
    ):
        if timeout <= 0 or interval <= 0:
            raise ValueError("Timeout and poll interval must be positive")
        settings = target_settings(target)
        if client.config.host.rstrip("/") != settings["host"]:
            raise ValueError("Execution is restricted to the selected Lab 9 workspace")
        self.target = target
        self.host = settings["host"]
        self.client = client
        self.inventory = inventory
        self.record = Report(report_path, host=self.host, target=target)
        self.path = self.record.path
        self.report = self.record.data
        self.timeout, self.interval = timeout, interval
        self.clock, self.sleep = clock, sleep
        self.started = clock()
        self.active_job = None
        self.active_pipeline = None

    def save(self):
        self.record.save(self.clock() - self.started)

    def remaining(self):
        return self.timeout - (self.clock() - self.started)

    def pause(self):
        if self.remaining() <= 0:
            raise TimeoutError("Automation deadline exceeded; inspect the saved report")
        self.sleep(min(self.interval, self.remaining()))

    def _poll(self, fetch, snapshot, terminal, stage):
        previous = None
        while True:
            if self.remaining() <= 0:
                raise TimeoutError("Monitoring deadline exceeded")
            value = fetch()
            current = snapshot(value)
            stage.update(current)
            self.save()
            if current != previous:
                emit(dict(stage))
                previous = current
            if current["state"] in terminal:
                return value
            self.pause()

    def wait_job(self, run_id, stage):
        run = self._poll(
            lambda: self.client.jobs.get_run(run_id=run_id),
            job_snapshot,
            JOB_TERMINAL,
            stage,
        )
        self.active_job = None
        outputs = {}
        for task in run.tasks or []:
            if not task.run_id:
                continue
            try:
                output = self.client.jobs.get_run_output(run_id=task.run_id)
            except DatabricksError as error:
                outputs[task.task_key] = {"output_error": str(error)}
                continue
            task_output = {}
            outputs[task.task_key] = task_output
            if output.error:
                task_output["error"] = output.error
                task_output["trace"] = (output.error_trace or "")[-4000:]
            if output.notebook_output and output.notebook_output.result:
                raw = output.notebook_output.result
                try:
                    task_output["result"] = json.loads(raw)
                except ValueError:
                    task_output["result"] = raw
        stage["outputs"] = outputs
        self.save()
        if stage["result"] != "SUCCESS":
            raise RuntimeError(
                f"Job run {run_id} ended with {stage['result'] or stage['state']}"
            )

    def run_job(self, key, commit):
        if self.remaining() <= 0:
            raise TimeoutError("Deadline exceeded before job submission")
        stage = {"kind": "job", "job_id": self.inventory[key], "name": key}
        self.report["stages"].append(stage)
        run = self.client.jobs.run_now(
            job_id=self.inventory[key],
            job_parameters={"release_commit": commit},
            idempotency_token=self.report["operation_id"] + "-" + key,
        )
        if not run.run_id:
            raise RuntimeError("Job submission returned no run ID")
        self.active_job = run.run_id
        stage["run_id"] = run.run_id
        self.save()
        self.wait_job(run.run_id, stage)

    def run_pipeline(self):
        if self.remaining() <= 0:
            raise TimeoutError("Deadline exceeded before pipeline submission")
        pipeline_id = self.inventory["pipeline_id"]
        stage = {"kind": "pipeline", "pipeline_id": pipeline_id}
        self.report["stages"].append(stage)
        # No application-level retry: pipeline updates have no idempotency token.
        update = self.client.pipelines.start_update(
            pipeline_id=pipeline_id, full_refresh=False
        )
        if not update.update_id:
            raise RuntimeError("Pipeline submission returned no update ID")
        self.active_pipeline = (pipeline_id, update.update_id)
        stage["update_id"] = update.update_id
        stage["url"] = f"{self.host}/pipelines/{pipeline_id}/updates/{update.update_id}"
        self.save()
        self._poll(
            lambda: (
                self.client.pipelines.get_update(
                    pipeline_id=pipeline_id, update_id=stage["update_id"]
                ).update
            ),
            pipeline_snapshot,
            PIPELINE_TERMINAL,
            stage,
        )
        self.active_pipeline = None
        if stage["state"] != "COMPLETED":
            raise RuntimeError(f"Pipeline update ended with {stage['state']}")

    def submit_notebook(self):
        stage = {
            "kind": "submitted_notebook",
            "notebook_path": self.inventory["probe_path"],
        }
        self.report["stages"].append(stage)
        run = self.client.jobs.submit(
            run_name="lab9-wikipedia-compute-probe",
            idempotency_token=self.report["operation_id"],
            timeout_seconds=int(self.timeout),
            tasks=[
                SubmitTask(
                    task_key="probe",
                    max_retries=0,
                    timeout_seconds=int(self.timeout),
                    notebook_task=NotebookTask(
                        notebook_path=self.inventory["probe_path"],
                    ),
                )
            ],
        )
        if not run.run_id:
            raise RuntimeError("Notebook submission returned no run ID")
        self.active_job = run.run_id
        stage["run_id"] = run.run_id
        self.save()
        self.wait_job(run.run_id, stage)

    def cancel_owned_work(self):
        cleanup = []
        if self.active_job:
            try:
                self.client.jobs.cancel_run(run_id=self.active_job)
                cleanup.append({"run_id": self.active_job, "cancellation": "requested"})
            except Exception as error:
                cleanup.append(
                    {"run_id": self.active_job, "cancellation_error": str(error)}
                )
        if self.active_pipeline:
            pipeline_id, update_id = self.active_pipeline
            try:
                # stop affects the pipeline; confirm our update is still active first.
                current = self.client.pipelines.get(pipeline_id=pipeline_id)
                latest = (current.latest_updates or [None])[0]
                if (
                    latest
                    and latest.update_id == update_id
                    and state_name(latest.state) not in PIPELINE_TERMINAL
                ):
                    self.client.pipelines.stop(pipeline_id=pipeline_id)
                    cleanup.append(
                        {"update_id": update_id, "cancellation": "requested"}
                    )
            except Exception as error:
                cleanup.append(
                    {"update_id": update_id, "cancellation_error": str(error)}
                )
        self.report["cleanup"] = cleanup

    def run(self, commit):
        self.run_job("setup_job_id", commit)
        self.run_pipeline()
        self.run_job("gold_job_id", commit)

    def execute(self, command, commit="local"):
        commands = {
            "run": lambda: self.run(commit),
            "pipeline": self.run_pipeline,
            "submit-notebook": self.submit_notebook,
        }
        if command not in commands:
            raise ValueError("Unknown automation command")
        manage_signals = threading.current_thread() is threading.main_thread()
        previous_sigterm = signal.getsignal(signal.SIGTERM) if manage_signals else None

        def terminate(signum, frame):
            # Let the first signal enter cleanup; repeated SIGTERM must not interrupt it.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            raise KeyboardInterrupt("Received SIGTERM")

        if manage_signals:
            signal.signal(signal.SIGTERM, terminate)
        self.report["command"] = command
        try:
            self.save()
            self.report["identity"] = preflight(
                self.client, self.inventory, self.target
            )
            commands[command]()
            self.report["status"] = "SUCCESS"
        except (Exception, KeyboardInterrupt) as error:
            if manage_signals:
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
            self.report["status"] = (
                "CANCELED" if isinstance(error, KeyboardInterrupt) else "FAILED"
            )
            self.report["error"] = str(error) or "Interrupted by user"
            self.cancel_owned_work()
        finally:
            try:
                self.record.finish()
                self.save()
                self.record.emit_summary()
            finally:
                if manage_signals:
                    signal.signal(signal.SIGTERM, previous_sigterm)
        return 0 if self.report["status"] == "SUCCESS" else 1
