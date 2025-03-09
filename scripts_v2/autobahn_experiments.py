from experiments import Experiment, copy_file_from_remote_public_ip
import os

class AutobahnExperiment(Experiment):
    def copy_back_build_files(self):
        remote_repo = f"/home/{self.dev_ssh_user}/repo/autobahn"
        TARGET_BINARIES = ["node", "benchmark_client"]

        # Copy the target/release to build directory
        for bin in TARGET_BINARIES:
            copy_file_from_remote_public_ip(f"{remote_repo}/target/release/{bin}", os.path.join(self.local_workdir, "build", bin), self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)

