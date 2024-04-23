################################################################
#                   Setup Kestrel Jupyter Kernel
#
# This module setups the Kestrel Jupyter kernel:
#   1. install the kernel to Jupyter environment (local env)
#   2. generate codemirror mode for Kestrel based on the
#      installed kestrel Python package for syntax highlighting
#   3. install the codemirror mode into Jupyter
#
# Install: pip will install the utility `kestrel_jupyter_setup`
#
# Usage: `kestrel_jupyter_setup`
#
################################################################

import os
import tempfile
import importlib.resources
import importlib.util
import shutil
import json
from jupyter_client.kernelspec import KernelSpecManager
from kestrel_jupyter_kernel.codemirror.setup import update_codemirror_mode

_KERNEL_SPEC = {
    "argv": ["python3", "-m", "kestrel_jupyter_kernel", "-f", "{connection_file}"],
    "display_name": "Kestrel",
    "language": "kestrel",
}


def install_kernelspec():
    with tempfile.TemporaryDirectory() as tmp_dirname:
        kernel_dirname = os.path.join(tmp_dirname, "kestrel_kernel")
        os.mkdir(kernel_dirname)
        kernel_filename = os.path.join(kernel_dirname, "kernel.json")
        with open(kernel_filename, "w") as kf:
            json.dump(_KERNEL_SPEC, kf)

        # prepare logo file
        try:
            # Python >=3.9
            logo_path = importlib.resources.files("kestrel_jupyter_kernel").joinpath(
                "logo-64x64.png"
            )
        except AttributeError:
            # Python 3.8
            pkg_init_file = importlib.util.find_spec("kestrel_jupyter_kernel").origin
            logo_path = os.path.join(os.path.dirname(pkg_init_file), "logo-64x64.png")
        finally:
            logo_filename = os.path.join(kernel_dirname, "logo-64x64.png")
            shutil.copyfile(logo_path, logo_filename)

        m = KernelSpecManager()
        m.install_kernel_spec(kernel_dirname, "kestrel", user=True)


def run():
    print("Setup Kestrel Jupyter Kernel")
    print("  Install new Jupyter kernel ...", end=" ")
    try:
        install_kernelspec()
    except:
        print("failed to install Kestrel Jupyter kernel")
    else:
        print("done")

    # generate and install kestrel codemirrmor mode
    print("  Compute and install syntax highlighting ...", end=" ")
    try:
        update_codemirror_mode()
    except:
        print("failed to setup syntax highlighting; known issue with JupyterLab")
    else:
        print("done")
