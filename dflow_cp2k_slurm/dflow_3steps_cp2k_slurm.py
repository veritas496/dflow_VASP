from dflow import SlurmRemoteExecutor

from dflow import (
    upload_artifact,
    Workflow,
    Step
)

from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact, 
    Slices
)
import os
from typing import List
from pathlib import Path
import subprocess

class CP2KOpt(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "Opt_input": Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "Opt_output": Artifact(Path)
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        cwd = os.getcwd()
        os.chdir(op_in["Opt_input"]) 
        cmd = "your cp2k exec environment"
        subprocess.call(cmd, shell=True)
        os.chdir(cwd)        
        return OPIO({
            "Opt_output": op_in["Opt_input"]/"output.out"
        })

class CP2KSingle(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'Single_input': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "Single_output": Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        cwd = os.getcwd()
        os.chdir(op_in["Single_input"])
        cmd = "your cp2k exec environment"
        subprocess.call(cmd, shell=True)
        os.chdir(cwd)
        return OPIO({
            "Single_output": op_in["Single_input"]/"output.out"
        })

def main():
    slurm_remote_executor = SlurmRemoteExecutor(
    host="your host", 
    port=port, 
    username="your username",  
    password="your password", 
    header="#!/bin/bash\n#SBATCH --nodes=1\n#SBATCH --ntasks-per-node=128\n#SBATCH --partition=cpu\n#SBATCH -e test.err\n", 
    )

    Structure_Opt = Step(
        "Structure-Opt",
        PythonOPTemplate(CP2KOpt),
        artifacts={"Opt_input": upload_artifact(["./cp2k_opt"])},
        executor=slurm_remote_executor,
    )

    CP2K_Electronic = Step(
        "CP2K-Electronic",
        PythonOPTemplate(CP2KSingle, slices=Slices("{{item}}", input_artifact=["Single_input"], output_artifact=["Single_output"])),
        artifacts={"Single_input": upload_artifact(["cp2k_elf", "cp2k_density"])}, # your input dir
        with_param=range(2),
        key="CP2KSingle-{{item}}",
        executor=slurm_remote_executor,
    )

    wf = Workflow(name="cp2k-task")
    wf.add(Structure_Opt)
    wf.add(CP2K_Electronic)
    wf.submit()

if __name__ == "__main__":
    main()

