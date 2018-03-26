# bdk

Task queue client for Yandex Tank, Volta and other runners

You should create config for bdk:

```(yaml)
configuration:
  api_address: "https://lpq.common-int.yandex-team.ru"
  api_claim_handler: "/claim.yaml"

executable:
  cmd: yandex-tank
  params:
    - "-c": "{job_config}"
    - "-o": "option.value"
  capabilities:
    executor_type: 'phantom'
```

Executable
===
At section `executable` you can specify executable file and its options. 
`Cmd` is a binary file and `params` for options.  
There are special key `{job_config}` where claimed job's 'config' section will be passed in.  
Sample above will start (for each claimed job) binary `yandex-tank` with options:  

`yandex-tank -o option.value -c %path to file w/ claimed config%`
 
Capabilities
===
Describes capabilities of test runner, you can specify any options 
that your claim API should know about to give you job.  
Implicitly adding default capability `__fqdn` with socket.fqdn() value.    
After start, bdk will start polling `api_address`+`api_claim_handler` for jobs.   
Job is a yaml structure w/ `task_id` and `config` options.

