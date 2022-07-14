from dflow import config, s3_config
config["host"] = "http://39.106.93.187:32746"
config["k8s_api_server"] = "https://101.200.186.161:6443"
config["token"] = "eyJhbGciOiJSUzI1NiIsImtpZCI6IlhMRGZjbnNRemE4RGQyUXRMZG1MX3NXeG5TMzlQTnhnSHZkS1lGM25SODAifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJhcmdvIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6ImFyZ28tdG9rZW4tOGY4djkiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoiYXJnbyIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6IjBhNzI1N2JhLWZkZWQtNGI2OS05YWU2LTZhY2U0M2UxNjdlNiIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDphcmdvOmFyZ28ifQ.ocRNsp_sdjM7dKpg_rYwPOKATslbDebDo807rkaVcqOScFKl11tqHbCwsekq4BZSEw-MaZjWAVsdE5jgvqIggp2qczx1QPMkBGQzkPwR4h7HbxYPUKIHkjlcvtsl06jf7urfzARUiD_UTahEFQLkUeN800Qblp-zMFBNLF2Y7_wW867drmQxynG1ssQ8agdu7yDZpwwJz-qzMMZsuZ7QNtL0pPQP2Iw_5C6jlos-Al1m3bgUJ5phh7yt-PBqwnMwEPaZWkVy9zFJc5t4J0jFc9nRnrR8fEd_OgJTTgQjHxS2DyXj9ZRUlGJA-tmHGqIJ7nuScv3lKwbb8TkeABq5DA"
s3_config["endpoint"] = "39.106.93.187:30900"

from dflow import (
    InputParameter,
    OutputParameter,
    InputArtifact,
    OutputArtifact,
    upload_artifact,
    Workflow,
    Step
)
from dflow.plugins.lebesgue import LebesgueContext

from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact,
)
import os, time
from typing import List
from pathlib import Path

class VASPOpt(OP):
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
        os.chdir(op_in["Opt_input"]) # change into the input dir       
        # call vasp_std 
        os.system("bash -c 'source /opt/intel/oneapi/setvars.sh;  ulimit -s unlimited ; mpirun -n 16 vasp_std'")
        return OPIO({
            "Opt_output": op_in["Opt_input"]/"CONTCAR"
        })

class VASPWf(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "Contcar": Artifact(Path),
            "Wf_input": Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "Locpot": Artifact(Path)  
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        os.chdir(op_in["Wf_input"]) # change into the input dir   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!    
        # call vasp_std 
        os.system("cp ../../Contcar/opt/CONTCAR ./POSCAR")
        os.system("bash -c 'source /opt/intel/oneapi/setvars.sh;  ulimit -s unlimited ; mpirun -n 16 vasp_std'")
        return OPIO({
            "Locpot": op_in["Wf_input"]/"LOCPOT"  
        })

class VASPElf(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "Contcar": Artifact(Path),
            "Elf_input": Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "Elfcar": Artifact(Path)  
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        os.chdir(op_in["Elf_input"]) # change into the input dir       
        # call vasp_std 
        os.system("cp ../../Contcar/opt/CONTCAR ./POSCAR")
        os.system("bash -c 'source /opt/intel/oneapi/setvars.sh;  ulimit -s unlimited ; mpirun -n 16 vasp_std'")
        return OPIO({
            "Elfcar": op_in["Elf_input"]/"ELFCAR"  
        })


def main():
    lebesgue_context = LebesgueContext(
        username="your username",
        password="your password",
        executor="lebesgue_v2",
        extra={
                "scass_type":"c16_m32_cpu",
                "program_id":your program ID,
                "job_type":"container",
                "template_cover_cmd_escape_bug":"True"
        },
    )

    Structure_Opt = Step(
        "Structure-Opt",
        PythonOPTemplate(VASPOpt, image="dp-harbor-registry.cn-zhangjiakou.cr.aliyuncs.com/deepmd/vasp:5.4.4"),
        artifacts={"Opt_input": upload_artifact(["./opt"])},    #通过upload_artifact上传本地文件夹
    )

    Single_workfunction = Step(
        "Single-workfunction",
        PythonOPTemplate(VASPWf, image="dp-harbor-registry.cn-zhangjiakou.cr.aliyuncs.com/deepmd/vasp:5.4.4"),
        artifacts={
            "Contcar": Structure_Opt.outputs.artifacts["Opt_output"],
            "Wf_input": upload_artifact(["./wf"])
            }, 
    )

    Single_elf = Step(
        "Single-elf",
        PythonOPTemplate(VASPElf, image="dp-harbor-registry.cn-zhangjiakou.cr.aliyuncs.com/deepmd/vasp:5.4.4"),
        artifacts={
            "Contcar": Structure_Opt.outputs.artifacts["Opt_output"],
            "Elf_input": upload_artifact(["./elf"])
            }, 
    )

    wf = Workflow(name="vasp-task", context=lebesgue_context)
    wf.add(Structure_Opt)
    wf.add([Single_workfunction, Single_elf])
    wf.submit()

if __name__ == "__main__":
    main()

