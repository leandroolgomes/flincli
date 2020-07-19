from .flink.commands import flink

__all_commands = [apps, params, flink]

def register_commands(cli):
    for c in __all_commands:
        cli.add_command(c)
