from __future__ import absolute_import

from ipykernel.kernelapp import IPKernelApp


class HiveQLKernelApp(IPKernelApp):
    """
    The main kernel application, inheriting from the ipykernel
    """
    from .kernel import HiveQLKernel
    from .install import HiveqlKernelInstall, HiveqlKernelRemove
    kernel_class = HiveQLKernel

    # We override subcommands to add our own install & remove commands
    subcommands = {
        'install': (HiveqlKernelInstall,
                    HiveqlKernelInstall.description.splitlines()[0]),
        'remove': (HiveqlKernelRemove,
                   HiveqlKernelRemove.description.splitlines()[0]),
    }



def main():
    HiveQLKernelApp.launch_instance()

if __name__ == '__main__':
    main()
