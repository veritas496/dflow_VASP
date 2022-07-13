# Introduction

In this tutorial, dflow is used for theoretical simulations of electronic structure properties of materials based on cp2k. This automated workflow makes parallel computations possible and we can monitor the simulation process in front of the screen instead of manually submitting repetitive computational tasks.

# Methods and procedure

We use the example of using cp2k to simulate the electronic structure properties of Si8 bulk to illustrate how to implement an automated workflow based on dflow.
Typically, such computational tasks follow a fixed procedure and are performed using similar scripts. To illustrate, for geometry optimization and electronic structure properties (e.g. ELF and electron density) of Si8 body models, we have to submit tasks manually on a case-by-case basis.
- Get Si8_bulk.cif
- Write input.inp files for each cp2k calculation
- Submit geometry calculation manually
- Submit ELF calculation manually
- Submit electronic density calculation manually
- Check task status and results

However, with the help of dflow, we only need to specify our computational goals and ideas, and then guide dflow to do it automatically.

- Design the workflow
- Write dflow
- Submit dflow and check results
Naturally, it frees up productivity, freeing theoretical calculators from tedious computational steps and giving them more time to conceive new ideas. 
# How to do
After specifying the tasks, it is the time to wrting dflow codes, submit takes and check results in Argo UI. https://127.0.0.1:2746/workflows
## Writing dflow codes

### Import Dflow package and other packages

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
import os, time
from typing import List
from pathlib import Path
import subprocess

### Define the operation class

Geometry optimization class

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
        cmd = "ulimit -s unlimited; module load cp2k/8.2-gcc7.3.0-openmpi4.1.1; mpirun -np 4 cp2k.popt -i input.inp -o output.out"
        subprocess.call(cmd, shell=True)
        os.chdir(cwd)        
        return OPIO({
            "Opt_output": op_in["Opt_input"]/"output.out"
        })
        
Electronic structure calculation class (including ELF and density)

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
        cmd = "ulimit -s unlimited; module load cp2k/8.2-gcc7.3.0-openmpi4.1.1; mpirun -np 4 cp2k.popt -i input.inp -o output.out"
        subprocess.call(cmd, shell=True)
        os.chdir(cwd)
        return OPIO({
            "Single_output": op_in["Single_input"]/"output.out"
        })
        
Define main function

Calling slurm resources

def main():
    slurm_remote_executor = SlurmRemoteExecutor(
    host="your-host", 
    port=host-port, 
    username="your-user-name",  
    password="your-password", 
    header="script-header", 
    )
    
Geometry optimization step

    Structure_Opt = Step(
        "Structure-Opt",
        PythonOPTemplate(CP2KOpt, image="dptechnology/dflow"),
        artifacts={"Opt_input": upload_artifact(["./cp2k_opt"])},
        executor=slurm_remote_executor,
    )
    
Electronic structure calculation step

    CP2K_Electronic = Step(
        "CP2K-Electronic",
        PythonOPTemplate(CP2KSingle, image="dptechnology/dflow", slices=Slices("{{item}}", input_artifact=["Single_input"], output_artifact=["Single_output"])),
        artifacts={"Single_input": upload_artifact(["cp2k_elf", "cp2k_density"])}, # your input dir
        with_param=range(2),
        key="CP2KSingle-{{item}}",
        executor=slurm_remote_executor,
    )
    
Add dflow steps

    wf = Workflow(name="cp2k-task")
    wf.add(Structure_Opt)
    wf.add(CP2K_Electronic)
    wf.submit()
    
Run main function

if __name__ == "__main__":
    main()
    
## Pots
### Monitor the set up process
kubectl get pod -n argo

And then we can see pots status in the windows:

NAME                                  READY   STATUS    RESTARTS      AGE
argo-server-78f47df69f-7mvwm          1/1     Running   2 (37s ago)   56s
minio-5d6dccd444-f8mwb                1/1     Running   0             56s
postgres-869f7fbd7f-zzmw9             1/1     Running   0             56s
workflow-controller-586fdf9f6-fd52d   1/1     Running   1 (41s ago)   56s

As long as the aforementioned four pots are all ready, we can ingress Argo and minio pots.

Ingress Argo UI and minio pot
- Ingress Argo UI: we can connect to Argo UI website. 
- Ingress minio: we can upload and download files.

What we need do is to submit two codes in two cmd/terminal seperately.

kubectl -n argo port-forward deployment/argo-server 2746:2746
kubectl -n argo port-forward deployment/minio 9000:9000

Submit dflow 

It is the time for us to submit the codes we have finished just now. 
python dflow_cp2k.py

And then we can see workflow has been submitted (ID: cp2k-task-xxxxx) in the windows.
Check dflow status in Argo UI
