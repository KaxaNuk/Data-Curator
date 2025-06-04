"""
Debugger tools.
"""


def init(debug_port):
    """
    Initialize the Pycharm Remote Server debugger.

    Parameters
    ----------
    debug_port: int
        The port the debugger server is listening on

    Returns
    -------
    None
    """
    import pydevd_pycharm

    pydevd_pycharm.settrace(
        'host.docker.internal',
        port=debug_port,
        stdoutToServer=True,
        stderrToServer=True
    )
