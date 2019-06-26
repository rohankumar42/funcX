import globus_sdk
import parsl
import os

from parsl.config import Config
from parsl.app.app import python_app, bash_app
from parsl.launchers import SingleNodeLauncher
from parsl.channels import LocalChannel
from parsl.channels import SSHInteractiveLoginChannel
from parsl.providers import LocalProvider, CondorProvider
from parsl.executors import HighThroughputExecutor

# GlobusAuth-related secrets
SECRET_KEY = os.environ.get('secret_key')
GLOBUS_KEY = os.environ.get('globus_key')
GLOBUS_CLIENT = os.environ.get('globus_client')

FUNCX_URL = "https://funcx.org/"


def _load_auth_client():
    """Create an AuthClient for the portal

    No credentials are used if the server is not production

    Returns:
        (globus_sdk.ConfidentialAppAuthClient): Client used to perform GlobusAuth actions
    """
    if _prod:
        app = globus_sdk.ConfidentialAppAuthClient(GLOBUS_CLIENT,
                                                   GLOBUS_KEY)
    else:
        app = globus_sdk.ConfidentialAppAuthClient('', '')
    return app


def _get_parsl_config():
    """
    Get the parsl config.

    Returns:
        (parsl.config.Config): Parsl config to execute tasks.
    """

    # config = Config(
    #     executors=[
    #         HighThroughputExecutor(
    #             label="htex_local",
    #             worker_debug=False,
    #             poll_period=1,
    #             cores_per_worker=1,
    #             max_workers=1,
    #             provider=LocalProvider(
    #                 channel=LocalChannel(),
    #                 init_blocks=1,
    #                 max_blocks=1,
    #                 min_blocks=1,
    #             ),
    #         )
    #     ],
    #     strategy=None
    # )

    x509_proxy = 'x509up_u%s'%(os.getuid())

    wrk_init = '''
    export XRD_RUNFORKHANDLER=1
    source /cvmfs/sft.cern.ch/lcg/views/LCG_95apython3/x86_64-centos7-gcc7-opt/setup.sh
    export PATH=`pwd`/.local/bin:$PATH
    export PYTHONPATH=`pwd`/.local/lib/python3.6/site-packages:$PYTHONPATH

    export X509_USER_PROXY=`pwd`/%s
    mkdir -p ./coffea_parsl_condor
    '''%(x509_proxy)

    twoGB = 2048
    nproc = 4

    condor_cfg = '''
    transfer_output_files = coffea_parsl_condor
    RequestMemory = %d
    RequestCpus = %d
    ''' % (twoGB*nproc, nproc)

    xfer_files = ['%s/.local' % (os.environ['HOME'], ), '%s/%s' % (os.environ['HOME'], x509_proxy, )]

    #envs={'PYTHONPATH':'/afs/hep.wisc.edu/home/lgray/.local/lib/python3.6/site-packages:%s'%os.environ['PYTHONPATH'],
    #      'X509_USER_PROXY':'./%s'%x509_proxy,
    #      'PATH':'/afs/hep.wisc.edu/home/lgray/.local/bin:%s'%os.environ['PATH']}

    config = Config(
        executors=[
            HighThroughputExecutor(
                label="coffea_parsl_condor",
                address=address_by_hostname(),
                worker_debug=True,
                prefetch_capacity=0,
                cores_per_worker=1,
                max_workers=nproc,
                worker_logdir_root='./',
                provider=CondorProvider(
                    channel=LocalChannel(),
                    init_blocks=4,
                    max_blocks=4,
                    #init_blocks=32,
                    #max_blocks=64,
                    nodes_per_block=1,
                    worker_init = wrk_init,                
                    transfer_input_files=xfer_files,
                    scheduler_options=condor_cfg,
                    cmd_timeout=200
                ),
            )
        ],
        retries = 10,
        app_cache = True,
        strategy = 'simple'
    )

    return config
