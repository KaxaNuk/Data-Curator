"""
Debugger tools.
"""


def init(debug_port: int) -> None:
    """
    Initialize the Pycharm Remote Server debugger.

    Parameters
    ----------
    debug_port
        The port the debugger server is listening on

    Returns
    -------
    None
    """
    import pydevd_pycharm

    pydevd_pycharm.settrace(
        'host.docker.internal',
        port=debug_port,
        stdout_to_server=True,
        stderr_to_server=True
    )
